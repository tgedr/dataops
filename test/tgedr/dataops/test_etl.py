from tgedr.dataops.etl import Etl


class MyEtl(Etl):
    @Etl.inject_configuration
    def extract(self, input) -> None:
        return input

    def transform(self, input_2: str = 6) -> None:
        return input_2

    def load(self) -> None:
        self._configuration["output"] = 9


def test_config():
    etl = MyEtl({"input": 3})
    assert 3 == etl.extract()


def test_default_param():
    etl = MyEtl()
    assert 6 == etl.transform()


def test_static_run():
    config = {"input": 3}
    MyEtl(config).run()
    assert 9 == config["output"]
