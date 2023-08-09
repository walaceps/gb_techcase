import pandas as pd


#Arquivo de testes para tratamento das tabelas
def transform() -> bool:
    df_2017 = pd.read_excel(r'C:\\Users\walac\\OneDrive\Documentos\\TechCase\Sales\\assets\Base_2017.xlsx')
    df_2018 = pd.read_excel(r'C:\\Users\walac\\OneDrive\Documentos\\TechCase\Sales\\assets\Base_2018.xlsx')
    df_2019 = pd.read_excel(r'C:\\Users\walac\\OneDrive\Documentos\\TechCase\Sales\\assets\Base_2019.xlsx')

    sales_df = pd.concat([df_2019, df_2018, df_2017])
    sales_df['ANO_VENDA'] = pd.to_datetime(sales_df['DATA_VENDA']).dt.year
    sales_df['MES_VENDA'] = pd.to_datetime(sales_df['DATA_VENDA']).dt.month
    return sales_df

df = transform()
print(df)

print('TABELA 1')
tabela_1 = df.groupby(["ANO_VENDA","MES_VENDA",])["QTD_VENDA"].sum().reset_index()
print(tabela_1)


print('TABELA 2')
tabela_2 = df.groupby(["MARCA","LINHA",])["QTD_VENDA"].sum().reset_index()
print(tabela_2)


print('TABELA 3')
tabela_3 = df.groupby(["MARCA","ANO_VENDA","MES_VENDA"])["QTD_VENDA"].sum().reset_index()
print(tabela_3)

print('TABELA 4')
tabela_4 = df.groupby(["LINHA","ANO_VENDA","MES_VENDA"])["QTD_VENDA"].sum().reset_index()
print(tabela_4)
