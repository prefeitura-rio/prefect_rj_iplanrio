# -*- coding: utf-8 -*-
"""
Tasks para o pipeline de relatorio CVL
"""

import pendulum
import pandas as pd
from prefect import task


def identificar_sessoes_cliente(group):
        nova_sessao = []
        if group.empty:
            return pd.Series([], dtype=bool)

        ultimo_inicio_sessao = group.iloc[0]['inicio_datetime']
        for _, row in group.iterrows():
            if (row['inicio_datetime'] - ultimo_inicio_sessao).total_seconds() > 24 * 3600:
                nova_sessao.append(True)
                ultimo_inicio_sessao = row['inicio_datetime']
            else:
                nova_sessao.append(False)
        if nova_sessao:
            nova_sessao[0] = True
        return pd.Series(nova_sessao, index=group.index)


def estatisticas_semanais_sessoes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o número de sessões e clientes únicos por semana a partir de um DataFrame de interações.

    Args:
        df (pd.DataFrame): DataFrame com os dados de interações.

    Returns:
        pd.DataFrame: DataFrame com as estatísticas semanais de sessões
    """
    df['semana_ano'] = df['inicio_datetime'].dt.to_period('W')

    sessoes_por_semana = df.groupby(['semana_ano'])['nova_sessao'].sum().reset_index()
    sessoes_por_semana = sessoes_por_semana.rename(columns={'nova_sessao': 'numero_sessoes'})

    clientes_unicos_por_semana = df.groupby(['semana_ano'])['contato_telefone'].nunique().reset_index()
    clientes_unicos_por_semana = clientes_unicos_por_semana.rename(columns={'contato_telefone': 'clientes_unicos'})

    estatisticas_por_semana = pd.merge(sessoes_por_semana, clientes_unicos_por_semana, on=['semana_ano'])

    total_sessoes_por_semana = df.groupby('semana_ano')['nova_sessao'].sum().reset_index()
    total_sessoes_por_semana = total_sessoes_por_semana.rename(columns={'nova_sessao': 'numero_sessoes'})

    total_clientes_unicos_por_semana = df.groupby('semana_ano')['contato_telefone'].nunique().reset_index()
    total_clientes_unicos_por_semana = total_clientes_unicos_por_semana.rename(columns={'contato_telefone': 'clientes_unicos'})

    estatisticas_totais_por_semana = pd.merge(total_sessoes_por_semana, total_clientes_unicos_por_semana, on='semana_ano')
    estatisticas_totais_por_semana['Tipo de atendimento'] = 'Total'

    estatisticas_com_totais_semana = pd.concat([estatisticas_por_semana, estatisticas_totais_por_semana], ignore_index=True)
    estatisticas_com_totais_semana = estatisticas_com_totais_semana.dropna(subset='Tipo de atendimento')
    estatisticas_com_totais_semana = estatisticas_com_totais_semana.sort_values(['semana_ano'])

    return estatisticas_com_totais_semana


def estatisticas_mensais_sessoes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o número de sessões e clientes únicos por mês a partir de um DataFrame de interações.

    Args:
        df (pd.DataFrame): DataFrame com os dados de interações.

    Returns:
        pd.DataFrame: DataFrame com as estatísticas mensais de sessões
    """
    sessoes_por_mes = df.groupby(['mes_ano'])['nova_sessao'].sum().reset_index()
    sessoes_por_mes = sessoes_por_mes.rename(columns={'nova_sessao': 'numero_sessoes'})

    clientes_unicos_por_mes = df.groupby(['mes_ano'])['contato_telefone'].nunique().reset_index()
    clientes_unicos_por_mes = clientes_unicos_por_mes.rename(columns={'contato_telefone': 'clientes_unicos'})

    estatisticas_por_mes = pd.merge(sessoes_por_mes, clientes_unicos_por_mes, on=['mes_ano'])

    total_sessoes_por_mes = df.groupby('mes_ano')['nova_sessao'].sum().reset_index()
    total_sessoes_por_mes = total_sessoes_por_mes.rename(columns={'nova_sessao': 'numero_sessoes'})

    total_clientes_unicos_por_mes = df.groupby('mes_ano')['contato_telefone'].nunique().reset_index()
    total_clientes_unicos_por_mes = total_clientes_unicos_por_mes.rename(columns={'contato_telefone': 'clientes_unicos'})

    estatisticas_totais_por_mes = pd.merge(total_sessoes_por_mes, total_clientes_unicos_por_mes, on='mes_ano')
    estatisticas_totais_por_mes['Tipo de atendimento'] = 'Total'

    estatisticas_com_totais_mes = pd.concat([estatisticas_por_mes, estatisticas_totais_por_mes], ignore_index=True)
    estatisticas_com_totais_mes = estatisticas_com_totais_mes.dropna(subset='Tipo de atendimento')

    estatisticas_com_totais_mes = estatisticas_com_totais_mes.sort_values(['mes_ano'])

    return estatisticas_com_totais_mes


@task
def calculate_24h_sessions(df: pd.DataFrame, report_month: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calcula sessões de 24h e estatísticas mensais/semanais a partir de um DataFrame de interações.

    Args:
        df (pd.DataFrame): DataFrame com os dados de interações.
        report_month (str): Mês de referência para o relatório (YYYY-MM).

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: DataFrames com estatísticas mensais e tabela
        original com id_sessap_24h e se aquela mensagem corresponde a uma nova sessão (coluna "nova_sessao").
    """
    # Converter colunas de data e hora para datetime
    df['inicio_datetime'] = pd.to_datetime(df['mensagem_datahora'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
    df['inicio_datetime'] = df['inicio_datetime'].fillna(pd.to_datetime(df['mensagem_datahora'], format='%Y-%m-%d %H:%M:%S', errors='coerce'))

    # Se o comprimento for 12, insere '9' após os 4 primeiros caracteres
    # Caso contrário, mantém o valor original
    df['contato_telefone'] = df.apply(lambda row:
        row['contato_telefone'][:4] + '9' + row['contato_telefone'][4:]
        if pd.notnull(row['contato_telefone']) and len(str(row['contato_telefone'])) == 12
        else row['contato_telefone'], axis=1)
    
    # Ordenar os dados por cliente e data de início
    df = df.sort_values(by=['contato_telefone', 'inicio_datetime'])

    df['nova_sessao'] = df.groupby('contato_telefone', group_keys=False).apply(identificar_sessoes_cliente)
    df['id_sessao_24h'] = df['contato_telefone'] + '_' + df.groupby('contato_telefone')['nova_sessao'].astype(int).cumsum().astype(str)

    # Extrair o mês e ano do início da interação
    df['mes_ano'] = df['inicio_datetime'].dt.to_period('M')

    print(f"Dataframe after calculating 24h sessions: {df.head()}")
    print(f"Dataframe after calculating 24h sessions: {df.iloc[0]}")

    df_filtered = df[df['mes_ano'] == report_month]

    print(f"Dataframe after filtering for report month: {df_filtered.head()}")
    print(f"Dataframe after filtering for report month: {df_filtered.iloc[0]}")

    # Calcular as estatísticas mensais e semanais
    estatisticas_mensais = estatisticas_mensais_sessoes(df_filtered)
    # estatisticas_semanais = estatisticas_semanais_sessoes(df_filtered)
    
    return estatisticas_mensais, df_filtered


@task
def get_first_and_last_day_of_previous_month() -> tuple[str, str]:
    """
    Retorna o primeiro e o último dia do mês anterior como objetos date.
    """
    today = pendulum.now("America/Sao_Paulo").date()

    first_day_of_current_month = today.replace(day=1)
    first_day_of_previous_month = first_day_of_current_month.subtract(months=1)

    start_date = first_day_of_previous_month
    end_date = first_day_of_current_month.subtract(days=1)

    print(f"Running report for dates from {start_date} to {end_date}")
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
