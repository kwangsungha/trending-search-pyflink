import time
from typing import Any, Generator
from datetime import datetime
import geohash

from pyflink.common import Time, Duration, Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
    FilterFunction,
    MapFunction,
    AggregateFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows


'''
QC_WINDOW_SIZE = Time.minutes(60)
QC_WINDOW_SLIDE = Time.minutes(5)
EMA_WINDOW_SIZE = Time.minutes(60 * 2)
EMA_WINDOW_SLIDE = Time.minutes(30)
'''
QC_WINDOW_SIZE = Time.seconds(60)
QC_WINDOW_SLIDE = Time.seconds(20)
EMA_WINDOW_SIZE = Time.seconds(60 * 2)
EMA_WINDOW_SLIDE = Time.seconds(30)


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
        print(f"RAW - {ts_ms} {geo} {kw} {user}")
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
        yield f"QC - Window: {context.window()} Key: {key} Value: {elements[0] if elements else {}}"


class ExponentialMovingAverageProcessWindowFunction(ProcessWindowFunction):
    def __init__(self, alpha: float = 0.1, debug: bool = True) -> None:
        self.alpha = alpha
        self.descriptor = MapStateDescriptor("trending_search-eda", Types.STRING(), Types.FLOAT())
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

        if self.debug:
            print(f"EMA - Window: {context.window()} Key: {key}")
        for k in set(state_kv) | set(given_kv):
            if self.debug:
                print(f"EMA[{k}] = {self.alpha} * {float(given_kv.get(k, 0.0)):.02f} + {(1.0 - self.alpha)} * {state_kv.get(k, 0.0):.02f}")
            ema_kv[k] = self.alpha * float(given_kv.get(k, 0.0)) + (1.0 - self.alpha) * state_kv.get(k, 0.0)

        state.put_all(list(ema_kv.items()))

        if self.debug:
            d = {k: f"{v:.02f}" for k, v in ema_kv.items()}
            yield f"EMA - Window: {context.window()} Key: {key} Value: {d}"
        else:
            yield (key, ema_kv)


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

    query_count_ds = (
        preprocess_ds
        .window(SlidingEventTimeWindows.of(QC_WINDOW_SIZE, QC_WINDOW_SLIDE))
        .aggregate(
            QueryCountAggregate(),
            window_function=DebugProcessWindowFunction(),
        )
    )

    ema_ds = (
        preprocess_ds
        .window(SlidingEventTimeWindows.of(EMA_WINDOW_SIZE, EMA_WINDOW_SLIDE))
        .aggregate(
            QueryCountAggregate(),
            window_function=ExponentialMovingAverageProcessWindowFunction(alpha=0.1),
        )
    )

    query_count_ds.print()
    ema_ds.print()
    env.execute()


if __name__ == "__main__":
    main()
