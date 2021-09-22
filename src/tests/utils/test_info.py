from src.utils.info import carrega_yaml


def test_carrega_yaml():
    info = carrega_yaml("aquis_censo_escola.yml")

    assert isinstance(info, dict)
    assert "RENOMEIA_COLUNAS" in info
