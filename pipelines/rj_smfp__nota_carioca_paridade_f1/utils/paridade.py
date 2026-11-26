"""Relatório de paridade F1: lê o resumo (uma linha por regra e classe) e dá o veredito com tolerância zero.

Contrato do resumo (definido pela trilha do dbt): ``regra`` (``R-01``...), ``classe`` (``igual``,
``correcao_catalogada:<B-xx>`` ou ``divergente``), ``quantidade``, ``corte_nsu`` e ``gerado_em``. ``comparados`` de uma
regra é a soma de ``quantidade``. Reprova qualquer ``divergente``, qualquer regra com ``comparados == 0`` (inclusive a
esperada que nem aparece no resumo), mistura de cortes de NSU, corte diferente do pedido e resumo anterior ao dbt.
"""

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, assert_never

EXPECTED_RULES = tuple(f"R-{number:02d}" for number in range(1, 12))
"""Regras ``dbt`` da matriz de paridade (guia-nacional/04-regras-de-negocio.md §10): R-01 a R-11."""

CORRECTION_PREFIX = "correcao_catalogada:"


class ResumoInvalidoError(ValueError):
    """O resumo não segue o contrato definido pela trilha do dbt."""


class ParidadeReprovadaError(RuntimeError):
    """A paridade foi reprovada: há divergência ou regra sem comparação."""


@dataclass(frozen=True, slots=True)
class Igual:
    """Linhas idênticas nos dois lados."""


@dataclass(frozen=True, slots=True)
class Correcao:
    """Diferença esperada, catalogada.

    :param id: Id da correção no catálogo (``B-04``...).
    """

    id: str


@dataclass(frozen=True, slots=True)
class Divergente:
    """Diferença sem explicação no catálogo."""


Classe = Igual | Correcao | Divergente


@dataclass(frozen=True, slots=True)
class ResumoRow:
    """Uma linha do resumo, já validada."""

    regra: str
    classe: Classe
    quantidade: int
    corte_nsu: int
    gerado_em: datetime


@dataclass(frozen=True, slots=True)
class RuleReport:
    """Contagens de uma regra.

    :param regra: Id da regra.
    :param comparados: Soma de todas as classes.
    :param iguais: Linhas ``igual``.
    :param correcoes_catalogadas: Linhas por id de correção, ordenadas por id.
    :param divergentes: Linhas ``divergente``.
    """

    regra: str
    comparados: int
    iguais: int
    correcoes_catalogadas: dict[str, int]
    divergentes: int


@dataclass(frozen=True, slots=True)
class ParidadeReport:
    """Veredito da paridade.

    :param corte_nsu: Maior corte de NSU do resumo; ``None`` se o resumo está vazio.
    :param gerado_em: Hora de geração mais recente do resumo; ``None`` se vazio.
    :param regras: Uma entrada por regra, ordenadas por id.
    :param resultado: ``aprovado`` só sem nenhum motivo.
    :param motivos: Por que reprovou, uma frase por problema, citando a regra.
    """

    corte_nsu: int | None
    gerado_em: datetime | None
    regras: tuple[RuleReport, ...]
    resultado: Literal["aprovado", "reprovado"]
    motivos: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        """Relatório no formato do contrato, com as chaves em ordem fixa."""
        return {
            "corte_nsu": self.corte_nsu,
            "gerado_em": self.gerado_em.astimezone(UTC).isoformat() if self.gerado_em else None,
            "regras": [
                {
                    "regra": item.regra,
                    "comparados": item.comparados,
                    "iguais": item.iguais,
                    "correcoes_catalogadas": dict(item.correcoes_catalogadas),
                    "divergentes": item.divergentes,
                }
                for item in self.regras
            ],
            "resultado": self.resultado,
            "motivos": list(self.motivos),
        }

    def to_json(self) -> str:
        """JSON determinístico: mesma entrada, mesmo texto."""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


def _parse_classe(value: object) -> Classe:
    if value == "igual":
        return Igual()
    if value == "divergente":
        return Divergente()
    if isinstance(value, str) and value.startswith(CORRECTION_PREFIX):
        correction_id = value.removeprefix(CORRECTION_PREFIX).strip()
        if not correction_id:
            raise ResumoInvalidoError(f"{CORRECTION_PREFIX} sem o id da correção: {value!r}")
        return Correcao(correction_id)
    raise ResumoInvalidoError(f"Classe fora do contrato (igual, {CORRECTION_PREFIX}<B-xx>, divergente): {value!r}")


def _parse_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ResumoInvalidoError(f"{name} deveria ser inteiro: {value!r}")
    return value


def parse_resumo(rows: Iterable[Mapping[str, object]]) -> tuple[ResumoRow, ...]:
    """Valida as linhas lidas do BigQuery e as converte para o tipo do domínio.

    :param rows: Linhas com ``regra``, ``classe``, ``quantidade``, ``corte_nsu`` e ``gerado_em``.
    :returns: Linhas validadas.
    :raises ResumoInvalidoError: Se alguma linha fugir do contrato (classe desconhecida, quantidade negativa...).
    """
    parsed = []
    for row in rows:
        regra, generated = row["regra"], row["gerado_em"]
        if not isinstance(regra, str) or not regra:
            raise ResumoInvalidoError(f"regra deveria ser texto não vazio: {regra!r}")
        if not isinstance(generated, datetime) or generated.tzinfo is None:
            raise ResumoInvalidoError(f"gerado_em deveria ser timestamp com fuso: {generated!r}")
        quantidade = _parse_int("quantidade", row["quantidade"])
        if quantidade < 0:
            raise ResumoInvalidoError(f"quantidade negativa em {regra}: {quantidade}")
        parsed.append(
            ResumoRow(
                regra, _parse_classe(row["classe"]), quantidade, _parse_int("corte_nsu", row["corte_nsu"]), generated
            )
        )
    return tuple(parsed)


def _rule_report(regra: str, rows: Sequence[ResumoRow]) -> RuleReport:
    iguais = divergentes = 0
    corrections: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        match row.classe:
            case Igual():
                iguais += row.quantidade
            case Correcao(id=correction_id):
                corrections[correction_id] += row.quantidade
            case Divergente():
                divergentes += row.quantidade
            case unreachable:
                assert_never(unreachable)
    return RuleReport(
        regra=regra,
        comparados=sum(row.quantidade for row in rows),
        iguais=iguais,
        correcoes_catalogadas=dict(sorted(corrections.items())),
        divergentes=divergentes,
    )


def _rule_reasons(item: RuleReport, present: bool) -> list[str]:
    reasons = []
    if not present:
        reasons.append(f"{item.regra}: ausente no resumo (comparados=0)")
    elif item.comparados == 0:
        reasons.append(f"{item.regra}: comparados=0")
    if item.divergentes > 0:
        reasons.append(f"{item.regra}: divergente={item.divergentes}")
    return reasons


def _global_reasons(
    rows: Sequence[ResumoRow], requested_corte_nsu: int | None, not_before: datetime | None
) -> list[str]:
    reasons = []
    cuts = sorted({row.corte_nsu for row in rows})
    if len(cuts) > 1:
        reasons.append(f"resumo mistura cortes de NSU: {', '.join(map(str, cuts))}")
    if requested_corte_nsu is not None and cuts and cuts != [requested_corte_nsu]:
        reasons.append(f"corte_nsu do resumo ({', '.join(map(str, cuts))}) diferente do pedido ({requested_corte_nsu})")
    if not_before is not None and rows and min(row.gerado_em for row in rows) < not_before:
        reasons.append(f"resumo gerado em {min(row.gerado_em for row in rows).isoformat()}, anterior ao início do dbt")
    return reasons


def build_report(
    rows: Sequence[ResumoRow],
    requested_corte_nsu: int | None = None,
    not_before: datetime | None = None,
    expected_rules: Sequence[str] = EXPECTED_RULES,
) -> ParidadeReport:
    """Agrega o resumo por regra e por classe e dá o veredito.

    :param rows: Linhas validadas por :func:`parse_resumo`.
    :param requested_corte_nsu: Corte de NSU pedido ao dbt; um resumo de outro corte reprova.
    :param not_before: Início do run de dbt (UTC); um resumo gerado antes dele é de outra execução e reprova.
    :param expected_rules: Regras que precisam aparecer no resumo.
    :returns: Relatório com uma entrada por regra esperada ou presente, ordenadas por id.
    """
    by_rule: dict[str, list[ResumoRow]] = defaultdict(list)
    for row in rows:
        by_rule[row.regra].append(row)
    reports = tuple(_rule_report(regra, by_rule.get(regra, ())) for regra in sorted({*expected_rules, *by_rule}))
    reasons = _global_reasons(rows, requested_corte_nsu, not_before)
    for item in reports:
        reasons.extend(_rule_reasons(item, present=item.regra in by_rule))
    return ParidadeReport(
        corte_nsu=max((row.corte_nsu for row in rows), default=None),
        gerado_em=max((row.gerado_em for row in rows), default=None),
        regras=reports,
        resultado="reprovado" if reasons else "aprovado",
        motivos=tuple(reasons),
    )
