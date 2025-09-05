# requisicao_pix.py
# Usa os templates de URL que você enviou e faz loop 2023-2025
import os
import time
import requests
import pandas as pd

# pasta de saída
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_and_save(url: str, filename: str, pause: float = 0.25):
    """Busca a URL, converte 'value' em DataFrame e salva CSV se houver dados."""
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        print(f"❌ Erro HTTP ao acessar URL para {filename}: {e}")
        return False

    try:
        payload = resp.json()
    except Exception as e:
        print(f"❌ Erro ao decodificar JSON ({filename}): {e}")
        return False

    data = payload.get("value", [])
    if not data:
        print(f"⚠️ Vazio (0 registros) para {filename} — URL consultada: {url}")
        return False

    df = pd.DataFrame(data)
    path = os.path.join(DATA_DIR, filename)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅ Salvo {filename} ({len(df)} linhas)")
    time.sleep(pause)
    return True

# --------------------
# templates (exatos como você enviou)
# --------------------
TEMPLATE_EST = (
    "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/"
    "EstatisticasTransacoesPix(Database=@Database)?@Database='{periodo}'&$top=10000&$format=json"
    "&$select=AnoMes,PAG_PFPJ,REC_PFPJ,PAG_REGIAO,REC_REGIAO,PAG_IDADE,REC_IDADE,FORMAINICIACAO,NATUREZA,FINALIDADE,VALOR,QUANTIDADE"
)

TEMPLATE_CHAVES = (
    "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/"
    "ChavesPix(Data=@Data)?@Data='{data}'&$top=10000&$format=json"
    "&$select=Data,ISPB,Nome,NaturezaUsuario,TipoChave,qt"
)

TEMPLATE_MUN = (
    "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/"
    "TransacoesPixPorMunicipio(DataBase=@DataBase)?@DataBase='{periodo}'&$top=10000&$format=json"
    "&$select=AnoMes,Municipio_Ibge,Municipio,Estado_Ibge,Estado,Sigla_Regiao,Regiao,VL_PagadorPF,QT_PagadorPF,VL_PagadorPJ,QT_PagadorPJ,"
    "VL_RecebedorPF,QT_RecebedorPF,VL_RecebedorPJ,QT_RecebedorPJ,QT_PES_PagadorPF,QT_PES_PagadorPJ,QT_PES_RecebedorPF,QT_PES_RecebedorPJ"
)

# --------------------
# loop 2023..2025
# --------------------
def main():
    anos = range(2023, 2026)  # 2023,2024,2025

    # Se quiser um arquivo único para transações por município, pode descomentar
    # abaixo para tentar pegar tudo de uma vez (mas aqui vamos por mês conforme pediu)
    # >>> opcional: url_all_mun = "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/TransacoesPixPorMunicipio?$top=10000&$format=json"
    # fetch_and_save(url_all_mun, "transacoes_municipio_geral.csv")

    for ano in anos:
        for mes in range(1, 13):
            periodo = f"{ano}{mes:02d}"           # AAAAMM
            data = f"{ano}-{mes:02d}-01"         # YYYY-MM-01

            # Estatísticas (por periodo AAAAMM)
            url_est = TEMPLATE_EST.format(periodo=periodo)
            nome_est = f"estatisticas_transacoes_{periodo}.csv"
            print(f"\n🔄 Estatísticas {periodo} ...")
            fetch_and_save(url_est, nome_est)

            # Estoque de chaves (por data YYYY-MM-01)
            url_ch = TEMPLATE_CHAVES.format(data=data)
            nome_ch = f"estoque_chaves_{periodo}.csv"
            print(f"🔄 Chaves {data} ...")
            fetch_and_save(url_ch, nome_ch)

            # Transacoes por municipio (por periodo AAAAMM)
            url_mun = TEMPLATE_MUN.format(periodo=periodo)
            nome_mun = f"transacoes_municipio_{periodo}.csv"
            print(f"🔄 Transacoes municipio {periodo} ...")
            fetch_and_save(url_mun, nome_mun)

    print("\n🎉 Coleta concluída (algumas consultas podem ter retornado vazias ou erro).")

if __name__ == "__main__":
    main()
