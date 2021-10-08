from mrtools.singleton import SingletonMetaClass


class TestSingleton(metaclass=SingletonMetaClass):
    pass


def test_singleton() -> None:

    instance1 = TestSingleton()
    instance2 = TestSingleton()

    assert instance1 is instance2
