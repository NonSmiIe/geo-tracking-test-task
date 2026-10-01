from geo_tracking.ingest import Acknowledgements


def test_acknowledgement_counts_only_the_contiguous_durable_prefix() -> None:
    acks = Acknowledgements()
    first, second, third = acks.issue(), acks.issue(), acks.issue()
    acks.settle(third)
    assert acks.count == 0 and not acks.idle.is_set()
    acks.settle(first)
    assert acks.count == 1
    acks.settle(second)
    assert acks.count == 3 and acks.idle.is_set()
