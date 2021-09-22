import os
import re
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from src.aquisicao.inep.censo_escola import EscolaETL


class TestEscolaETL(unittest.TestCase):
    base_path: Path = None
    test_path: Path = None
    dados_path: Path = None
    saida_path: Path = None
    inter_path: Path = None
    etl: EscolaETL = None

    @classmethod
    def setUpClass(cls):
        cls.base_path = Path(os.path.dirname(__file__))
        cls.test_path = cls.base_path.parent
        cls.dados_path = cls.test_path / "dados"
        cls.saida_path = cls.test_path / "saida"
        cls.inter_path = cls.base_path / "intermediario"

        cls.etl = EscolaETL(
            entrada=str(cls.dados_path),
            saida=str(cls.saida_path / "aquisicao"),
        )

        cls.etl._inep = {k: "" for k in os.listdir(cls.dados_path / "censo_escolar")}

        cls.bases = {
            f"{k[:4]}.zip": pd.read_parquet(cls.inter_path / k)
            for k in os.listdir(cls.inter_path)
            if "escolas_20" in k
        }

    def test_extract(self) -> None:
        self.etl.extract()

        self.assertTrue(self.etl.dados_entrada is not None)
        self.assertEqual(
            set(os.listdir(self.dados_path / "censo_escolar")),
            set(self.etl.dados_entrada),
        )
        self.assertIsInstance(self.etl.dados_entrada["2020.zip"], pd.DataFrame)

    def test_renomeia_colunas(self) -> None:
        self.etl._dados_entrada = self.bases

        renomear = {
            k: set(self.etl._configs["RENOMEIA_COLUNAS"]).intersection(set(d.columns))
            for k, d in self.etl.dados_entrada.items()
        }

        self.etl.renomeia_colunas()

        for k, cols in renomear.items():
            for c in cols:
                d = self.etl.dados_entrada[k]
                self.assertFalse(c in d)
                self.assertTrue(self.etl._configs["RENOMEIA_COLUNAS"][c] in d)

    def test_dropa_colunas(self) -> None:
        self.etl._dados_entrada = self.bases
        self.etl.renomeia_colunas()
        self.etl.dropa_colunas()

        for d in self.etl.dados_entrada.values():
            for c in self.etl._configs["DROPAR_COLUNAS"]:
                self.assertFalse(c in d)

    def test_processa_dt(self) -> None:
        self.etl._dados_entrada = self.bases
        self.etl.renomeia_colunas()
        self.etl.dropa_colunas()
        self.etl.processa_dt()

        for d in self.etl.dados_entrada.values():
            for c in d:
                if c.startswith("DT_"):
                    self.assertTrue(d[c].dtype == "datetime64[ns]")

    def test_processa_qt(self) -> None:
        self.etl._dados_entrada = self.bases
        self.etl.renomeia_colunas()
        self.etl.dropa_colunas()
        self.etl.processa_dt()
        self.etl.processa_qt()

        for k, d in self.etl.dados_entrada.items():
            if k >= "2019.zip":
                for c in self.etl._configs["COLS_88888"]:
                    if c in d:
                        self.assertFalse(88888 in d[c].values)

    def test_processa_in(self) -> None:
        self.etl._dados_entrada = self.bases
        self.etl.renomeia_colunas()
        self.etl.dropa_colunas()
        self.etl.processa_dt()
        self.etl.processa_qt()

        cols = set([c for d in self.etl.dados_entrada.values() for c in d])
        criar_qt = {
            k: set([c for c in d if c.startswith("QT_") and f"IN{c[2:]}" in cols])
            for k, d in self.etl.dados_entrada.items()
        }
        criar_comp = {
            k: set(
                [
                    criar
                    for criar, t in self.etl._configs["TRATAMENTO_IN"].items()
                    for c in d
                    if re.search(t[0], c)
                ]
            )
            for k, d in self.etl.dados_entrada.items()
        }

        self.etl.processa_in()

        for k, cols in criar_qt.items():
            self.assertTrue(cols.issubset(set(self.etl.dados_entrada[k])))

        for k, cols in criar_comp.items():
            self.assertTrue(cols.issubset(set(self.etl.dados_entrada[k])))

        for d in self.etl.dados_entrada.values():
            if "IN_ENERGIA_INEXISTENTE" in d:
                self.assertTrue("IN_ENERGIA_OUTROS" in d)
            if "TP_OCUPACAO_GALPAO" in d:
                self.assertTrue("IN_LOCAL_FUNC_GALPAO" in d)
            if "TP_INDIGENA_LINGUA" in d:
                self.assertTrue("IN_LINGUA_INDIGENA" in d)
                self.assertTrue("IN_LINGUA_PORTUGUESA" in d)
            if "IN_BIBLIOTECA" in d:
                self.assertTrue("IN_BIBLIOTECA_SALA_LEITURA" in d)
            if "IN_AGUA_FILTRADA" in d:
                self.assertTrue("IN_AGUA_POTAVEL" in d)

            for c in d:
                if c.startswith("IN_"):
                    self.assertTrue({0, 1, np.nan}, set(d[c].unique()))

    def test_processa_tp(self) -> None:
        self.etl._dados_entrada = self.bases
        self.etl.renomeia_colunas()
        self.etl.dropa_colunas()
        self.etl.processa_dt()
        self.etl.processa_qt()
        self.etl.processa_in()
        self.etl.processa_tp()

        for d in self.etl.dados_entrada.values():
            if "IN_LINGUA_INDIGENA" in d and "IN_LINGUA_PORTUGUESA" in d:
                self.assertTrue("TP_INDIGENA_LINGUA" in d)
                self.assertEqual(
                    {
                        np.nan,
                        "EM LÍNGUA INDÍGENA E EM LÍNGUA PORTUGUESA",
                        "SOMENTE EM LÍNGUA INDÍGENA",
                        "SOMENTE EM LÍNGUA PORTUGUESA",
                    },
                    set(d["TP_INDIGENA_LINGUA"]),
                )
            if "TP_OCUPACAO_GALPAO" in d:
                self.assertGreater(d["TP_OCUPACAO_GALPAO"].nunique(), 2)
            if "TP_OCUPACAO_PREDIO_ESCOLAR" in d:
                self.assertGreater(d["TP_OCUPACAO_PREDIO_ESCOLAR"].nunique(), 2)

            for c in self.etl._configs["DEPARA_TP"]:
                if c in d:
                    self.assertEqual("category", d[c].dtype)

    def test_concatena_bases(self) -> None:
        self.etl._dados_entrada = self.bases
        self.etl.renomeia_colunas()
        self.etl.dropa_colunas()
        self.etl.processa_dt()
        self.etl.processa_qt()
        self.etl.processa_in()
        self.etl.processa_tp()
        self.etl.concatena_bases()

        self.assertEqual({"escola_temp", "escola_atemp"}, set(self.etl.dados_saida))

        self.assertEqual(
            self.etl.dados_saida["escola_atemp"].shape[0],
            self.etl.dados_saida["escola_atemp"]["CO_ENTIDADE"].nunique(),
        )

        self.assertTrue(
            set(self.etl._configs["COLS_ATEMPORAL"]).issubset(
                set(self.etl.dados_saida["escola_atemp"])
            )
        )

    def test_preenche_nulos(self) -> None:
        self.etl._dados_entrada = self.bases
        self.etl.renomeia_colunas()
        self.etl.dropa_colunas()
        self.etl.processa_dt()
        self.etl.processa_qt()
        self.etl.processa_in()
        self.etl.processa_tp()
        self.etl.concatena_bases()

        antes = self.etl.dados_saida["escola_temp"].count()

        self.etl.preenche_nulos()

        for c in self.etl._configs["COLS_FBFILL"]:
            self.assertGreaterEqual(
                self.etl.dados_saida["escola_temp"][c].count(), antes[c]
            )

        for c in self.etl._configs["REMOVER_COLS"]:
            self.assertTrue(c not in self.etl.dados_saida["escola_temp"])

        for c in self.etl._configs["PREENCHER_NULOS"]:
            self.assertEqual(
                self.etl.dados_saida["escola_temp"].shape[0],
                self.etl.dados_saida["escola_temp"][c].count(),
            )

    def test_transform(self) -> None:
        pass


if __name__ == "__main__":
    unittest.main()
