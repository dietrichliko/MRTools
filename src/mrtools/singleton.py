"""Metaclass for Singleton

https://www.pythonprogramming.in/singleton-class-using-metaclass-in-python.html
"""


class SingletonMetaClass(type):
    """Metaclass for Singleton

    Usage:
        class MySingleton(metaclass=SingletonMetaClass):
            ...

        instance1 = MySingleton()
        instance2 = MySingleton()
        assert instance1 is instance2

    """

    def __init__(self, name, bases, dic):
        self.__single_instance = None
        super().__init__(name, bases, dic)

    def __call__(cls, *args, **kwargs):
        if cls.__single_instance:
            return cls.__single_instance
        single_obj = cls.__new__(cls)
        single_obj.__init__(*args, **kwargs)
        cls.__single_instance = single_obj
        return single_obj
