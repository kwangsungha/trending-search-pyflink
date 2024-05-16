import time
from typing import Any, Generator
from datetime import datetime
import geohash
from loguru import logger

from pyflink.common import Time, Duration, Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
    FilterFunction,
    MapFunction,
    AggregateFunction,
    ProcessWindowFunction,
    KeyedCoProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows


"""
QC_WINDOW_SIZE = Time.minutes(60)
QC_WINDOW_SLIDE = Time.minutes(5)
EMA_WINDOW_SIZE = Time.minutes(60 * 2)
EMA_WINDOW_SLIDE = Time.minutes(30)
"""
QC_WINDOW_SIZE = Time.seconds(60)
QC_WINDOW_SLIDE = Time.seconds(20)
EMA_WINDOW_SIZE = Time.seconds(60 * 2)
EMA_WINDOW_SLIDE = Time.seconds(60)


# fmt: off
raw_data = [
    {"timestamp": "2024-05-13 12:00:00", "search_keyword": "A", "hash": "user1", "lat": 37.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:00:01", "search_keyword": "A", "hash": "user1", "lat": 37.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:00:20", "search_keyword": "A", "hash": "user1", "lat": 37.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:00:30", "search_keyword": "B", "hash": "user1", "lat": 37.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:00:31", "search_keyword": "A", "hash": "user1", "lat": 37.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:00:32", "search_keyword": "B", "hash": "user1", "lat": 37.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:01:00", "search_keyword": "B", "hash": "user1", "lat": 37.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:01:03", "search_keyword": "A", "hash": "user1", "lat": 37.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:01:09", "search_keyword": "B", "hash": "user1", "lat": 37.0, "lng": 127.0, },

    {"timestamp": "2024-05-13 12:00:00", "search_keyword": "A", "hash": "user2", "lat": 38.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:00:01", "search_keyword": "A", "hash": "user2", "lat": 38.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:00:20", "search_keyword": "A", "hash": "user2", "lat": 38.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:00:30", "search_keyword": "B", "hash": "user2", "lat": 38.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:00:31", "search_keyword": "A", "hash": "user2", "lat": 38.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:00:32", "search_keyword": "B", "hash": "user2", "lat": 38.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:01:00", "search_keyword": "B", "hash": "user2", "lat": 38.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:01:03", "search_keyword": "A", "hash": "user2", "lat": 38.0, "lng": 127.0, },
    {"timestamp": "2024-05-13 12:01:09", "search_keyword": "B", "hash": "user2", "lat": 38.0, "lng": 127.0, },

]
# fmt: on


class DuplicateUserFilter(FilterFunction):
    def __init__(self) -> None:
        self.cache = {}

    def filter(self, value) -> bool:
        ts_ms, _, kw, user = value
        prev = self.cache.get((user, kw))
        self.cache[(user, kw)] = ts_ms
        if prev is not None and ts_ms - prev < 5000:
            return False
        return True


class ActoyoLogMapper(MapFunction):
    def __init__(self) -> None:
        self.precision = 5

    def map(self, line) -> tuple[Any, ...]:
        ts, kw, user, lat, lng = (
            line["timestamp"],
            line["search_keyword"],
            line["hash"],
            line["lat"],
            line["lng"],
        )
        ts_ms = int(
            time.mktime(datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timetuple()) * 1000
        )
        geo = geohash.encode(
            latitude=float(lat), longitude=float(lng), precision=self.precision
        )
        logger.debug(f"RAW - {ts_ms} {geo} {kw} {user}")
        return (ts_ms, geo, kw, user)


class FirstElementTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value[0])


class QueryCountAggregate(AggregateFunction):
    def create_accumulator(self) -> dict[str, int]:
        return {}

    def add(
        self, v: tuple[str, str, int], accumulator: dict[str, int]
    ) -> dict[str, int]:
        if v[1] in accumulator:
            accumulator[v[1]] += 1
        else:
            accumulator[v[1]] = 1
        return accumulator

    def get_result(self, accumulator: dict[str, int]) -> dict[str, int]:
        return accumulator

    def merge(self, x, y) -> dict[str, int]:
        return {k: x.get(k, 0) + y.get(k, 0) for k in set(x) | set(y)}


class DebugProcessWindowFunction(ProcessWindowFunction):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements,
    ):
        logger.debug(
            f"QC - Window: {context.window()} Key: {key} Value: {elements[0] if elements else {}}"
        )
        yield (key, elements[0] if elements else {})


class ExponentialMovingAverageProcessWindowFunction(ProcessWindowFunction):
    def __init__(self, alpha: float = 0.1, debug: bool = True) -> None:
        self.alpha = alpha
        self.descriptor = MapStateDescriptor(
            "trending_search-eda", Types.STRING(), Types.FLOAT()
        )
        self.debug = debug

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements,
    ) -> Generator[tuple[str, dict[str, float]], None, None]:
        state = context.global_state().get_map_state(self.descriptor)
        state_kv = dict(state.items())

        given_kv = elements[0] if elements else {}

        ema_kv = {}

        logger.debug(f"EMA - Window: {context.window()} Key: {key}")

        for k in set(state_kv) | set(given_kv):
            logger.debug(
                f"EMA[{k}] = {self.alpha} * {float(given_kv.get(k, 0.0)):.02f} + {(1.0 - self.alpha)} * {state_kv.get(k, 0.0):.02f}"
            )
            ema_kv[k] = self.alpha * float(given_kv.get(k, 0.0)) + (
                1.0 - self.alpha
            ) * state_kv.get(k, 0.0)

        state.put_all(list(ema_kv.items()))

        d = {k: f"{v:.02f}" for k, v in ema_kv.items()}
        logger.debug(f"EMA - Window: {context.window()} Key: {key} Value: {d}")

        yield (key, ema_kv)


class TrendingSearchKeyedCoProcessFunction(KeyedCoProcessFunction):
    def __init__(self, max_count: int = 20) -> None:
        self.latest_trend = None
        self.latest_ema = None
        self.max_count = max_count

    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ValueStateDescriptor("latest_ema", Types.PICKLED_BYTE_ARRAY())
        self.latest_ema = runtime_context.get_state(descriptor)

        descriptor = ValueStateDescriptor("latest_trend", Types.PICKLED_BYTE_ARRAY())
        self.latest_trend = runtime_context.get_state(descriptor)

    def process_element1(
        self, value: tuple[str, dict[str, float]], _: KeyedCoProcessFunction.Context
    ) -> Generator[tuple[str, list[dict[str, Any]]], None, None]:
        assert self.latest_ema is not None
        assert self.latest_trend is not None
        geo, qc = value
        latest_ema = self.latest_ema.value() or {}
        latest_trend = self.latest_trend.value() or []
        logger.debug(
            f"TS:QC  - Key: {geo} - Value: {qc} - EMA: {latest_ema} - Prev: {latest_trend}"
        )
        trend = self._trending_search(qc, latest_ema, latest_trend)
        self._update_latest_trend(trend)
        yield (geo, trend)

    def process_element2(
        self, value: tuple[str, dict[str, float]], _: KeyedCoProcessFunction.Context
    ) -> None:
        logger.debug(f"TS:EMA - Key: {value[0]} - Value: {value[1]}")
        self._update_latest_ema(value[1])

    def _trending_search(
        self,
        qc: dict[str, float],
        latest_ema: dict[str, float],
        latest_trend: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        trend_score = []
        for k, qc_v in qc.items():
            ema_v = latest_ema.get(k, 0.0)
            trend_score.append((k, qc_v - ema_v))

        trend_score = sorted(trend_score, key=lambda x: (x[0], -x[1]))[: self.max_count]
        return self._compare_trend(latest_trend, trend_score)

    def _compare_trend(
        self, prev_trend: list[dict[str, Any]], curr_score: list[tuple[str, float]]
    ) -> list[dict[str, Any]]:
        prev_ranks = {v["keyword"]: v["rank"] for v in prev_trend}
        ret = []
        for rank, (keyword, score) in enumerate(curr_score):
            v = {"rank": rank, "keyword": keyword, "score": score}
            prev_rank = prev_ranks.get(keyword, -1)
            if prev_rank < 0:
                v["symbol"] = "new"
            elif rank < prev_rank:
                v["symbol"] = "up"
            elif rank > prev_rank:
                v["symbol"] = "down"
            else:
                v["symbol"] = "-"

            ret.append(v)
        return ret

    def _update_latest_ema(self, ema: dict[str, float]) -> None:
        assert self.latest_ema is not None
        self.latest_ema.update(ema)

    def _update_latest_trend(self, trend: list[dict[str, Any]]) -> None:
        assert self.latest_trend is not None
        self.latest_trend.update(trend)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(1)

    ds = env.from_collection(raw_data)

    def split(line) -> Generator[Any, None, None]:
        yield line

    def geo_and_keyword_mapper(v: tuple[Any, ...]) -> tuple[str, str, int]:
        return (v[1], v[2], 1)  # (geo, keyword, 1)

    preprocess_ds = (
        ds.flat_map(split)
        .map(ActoyoLogMapper())
        .filter(DuplicateUserFilter())
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(
                Duration.of_seconds(20)
            ).with_timestamp_assigner(FirstElementTimestampAssigner())
        )
        .map(geo_and_keyword_mapper)
        .key_by(lambda x: x[0])
    )

    query_count_ds = preprocess_ds.window(
        SlidingEventTimeWindows.of(QC_WINDOW_SIZE, QC_WINDOW_SLIDE)
    ).aggregate(
        QueryCountAggregate(),
        window_function=DebugProcessWindowFunction(),
    )

    ema_ds = preprocess_ds.window(
        SlidingEventTimeWindows.of(EMA_WINDOW_SIZE, EMA_WINDOW_SLIDE)
    ).aggregate(
        QueryCountAggregate(),
        window_function=ExponentialMovingAverageProcessWindowFunction(alpha=0.1),
    )

    trending_search_ds = (
        query_count_ds.connect(ema_ds)
        .key_by(lambda x: x[0], lambda y: y[0])
        .process(TrendingSearchKeyedCoProcessFunction())
    )

    trending_search_ds.print()

    env.execute()


if __name__ == "__main__":
    main()
