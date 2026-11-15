import sys
import types


def test_generator_resumes_after_a_pause_instead_of_bursting(monkeypatch):
    """After the process is paused (a laptop sleeping), the generator must not send every
    missed tick at once: that burst made accounts look like they paid hundreds of times an hour."""
    produced = []

    class FakeProducer:
        def __init__(self, conf):
            pass

        def produce(self, topic, key=None, value=None):
            produced.append(topic)

        def poll(self, _):
            pass

        def flush(self, _):
            pass

    monkeypatch.setitem(sys.modules, "confluent_kafka", types.SimpleNamespace(Producer=FakeProducer))
    monkeypatch.setenv("ENVIRONMENT", "demo")
    from datetime import UTC, datetime

    from services.generator import generator as g

    clock = {"t": 0.0, "ticks": 0}
    sleeps = []

    def perf_counter():
        return clock["t"]

    def sleep(seconds):
        sleeps.append(clock["ticks"])
        clock["t"] += seconds

    real_tick = g.Simulator.tick

    def tick(self, dt):
        clock["ticks"] += 1
        if clock["ticks"] == 10:
            clock["t"] += 30.0          # the machine slept for 30 s here
        return real_tick(self, dt)

    monkeypatch.setattr(g.time, "perf_counter", perf_counter)
    monkeypatch.setattr(g.time, "sleep", sleep)
    monkeypatch.setattr(g.Simulator, "tick", tick)
    monkeypatch.setattr(g.Path, "write_text", lambda self, text: None)

    g.run_kafka(g.Simulator(g.Config(n_accounts=200), start=datetime(2026, 1, 1, tzinfo=UTC)), rate=10, max_events=60)

    # At 10 per second a 30 s pause is 300 missed ticks. Catching up would run every
    # remaining tick without sleeping; resuming means it paces again right away.
    after = [t for t in sleeps if t > 10]
    assert len(after) >= 45, f"only {len(after)} of the ticks after the pause were paced"
