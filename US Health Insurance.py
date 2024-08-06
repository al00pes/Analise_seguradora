# Databricks notebook source
# MAGIC %md
# MAGIC ### Introdução
# MAGIC
# MAGIC ste conjunto de dados contém 1338 linhas de dados segurados, onde as cobranças de seguro são dadas em relação aos seguintes atributos do segurado: Idade, Sexo, IMC, Número de filhos, Fumante e Região. Os atributos são uma mistura de variáveis ​​numéricas e categóricas.
# MAGIC
# MAGIC ### Objetivo
# MAGIC
# MAGIC Analisar os dados dos clientes e entender como posso utilizar as informações no negócio
# MAGIC
# MAGIC ### Dicionário do dados
# MAGIC
# MAGIC idade - Idade do beneficiário principal
# MAGIC sexo - Gênero do contratante de seguros, feminino / masculino
# MAGIC IMC - Índice de massa corporal, que fornece uma compreensão do corpo, pesos que são relativamente altos ou baixos
# MAGIC crianças - Número de crianças cobertas pelo seguro de saúde / Número de dependentes
# MAGIC fumante - Fumante / Não fumante
# MAGIC região - Area residencial do beneficiário nos EUA: nordeste, sudeste, sudoeste, noroeste
# MAGIC cobranças - Custos médicos individuais cobrados pelo seguro de saúde.

# COMMAND ----------

from pyspark.sql.functions import col, when, sum, avg, row_number
from pyspark.sql.functions import * 



# COMMAND ----------

# instanciando o dataset

dataset = 'dbfs:/FileStore/insurance.csv'

# COMMAND ----------

df_insarunce = spark\
                .read\
                .format("csv")\
                .option("inferschema",True)\
                .option("header",True)\
                .csv(dataset)

df_insarunce.show(5)      

# COMMAND ----------

# Exibindo o schema do dataframe
df_insarunce.printSchema()

# COMMAND ----------

#Exibindo as ultimas 5 linhas 

df_insarunce.tail(5)

# COMMAND ----------

#Contando o numero de linhas 
df_insarunce.count()

# COMMAND ----------

df_insarunce.describe().show()

# COMMAND ----------

#Verificando a distruição 
df_insarunce.select('smoker').distinct().collect()

# COMMAND ----------

#VErificação distruição da coluna sex
df_insarunce.select('sex').distinct().collect()

# COMMAND ----------


#Distribuição da região
df_insarunce.select('region').distinct().collect()

# COMMAND ----------

#Verificando missings no dataset
from pyspark.sql.functions import col, sum

df_nulos = df_insarunce.select([
    sum(col(c).isNull().cast("int")).alias(c) for c in df_insarunce.columns
    ])

df_nulos.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformação
# MAGIC
# MAGIC Nessa etapa irei modificar o nome das colunas e padronizar para ser um dataset com apenas valores numericos
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#Modificando o nome das colunas 
df_insarunce_01 = df_insarunce.withColumnRenamed("age","idade")\
                              .withColumnRenamed("sex","sexo")\
                              .withColumnRenamed("bmi","imc")\
                              .withColumnRenamed("smoker","fumante")\
                              .withColumnRenamed("region", "regiao")\
                              .withColumnRenamed("children","filho")\
                              .withColumnRenamed("expenses","Despesa")

#Validando a modificação
df_insarunce_01.columns


# COMMAND ----------

# Transformando a linhas categoricas em linhas numericas
#female (Feminino) = 0
#male (masculino) = 1
from pyspark.sql.functions import regexp_replace

df_insarunce_01 = df_insarunce_01.withColumn('sexo', regexp_replace(df_insarunce_01['sexo'],'female','1'))
df_insarunce_01 = df_insarunce_01.withColumn('sexo', regexp_replace(df_insarunce_01['sexo'],'male','2'))

df_insarunce_01.show(2)



# COMMAND ----------

#Transformando as regiões em colunas numericas
#northwest = 0
#southeast = 1
#northeast = 2
#southwest = 3

df_insarunce_01 = df_insarunce_01.withColumn('regiao', regexp_replace(df_insarunce_01['regiao'],'northwest','0'))
df_insarunce_01 = df_insarunce_01.withColumn('regiao', regexp_replace(df_insarunce_01['regiao'],'southeast','1'))
df_insarunce_01 = df_insarunce_01.withColumn('regiao', regexp_replace(df_insarunce_01['regiao'],'northeast','2'))
df_insarunce_01 = df_insarunce_01.withColumn('regiao', regexp_replace(df_insarunce_01['regiao'],'southwest','3'))


#validando a alteração
df_insarunce_01.select('regiao').distinct().collect()

# COMMAND ----------

#Transformando a coluna "fumante"

# no = 0
# yes = 1

df_insarunce_01 = df_insarunce_01.withColumn('fumante', regexp_replace(df_insarunce_01['fumante'],'no','0'))
df_insarunce_01 = df_insarunce_01.withColumn('fumante', regexp_replace(df_insarunce_01['fumante'],'yes','1'))

#Validando a modificação
df_insarunce_01.select('fumante').distinct().collect()

# COMMAND ----------

#Transformando a coluna IMC

# interpretação do IMC
# Entre 18,5 e 24,9 -	Normal = 0
# Entre 25,0 e 29,9	- Sobrepeso	I = 1
# Entre 30,0 e 39,9 - Obesidade	II = 2
# Maior que 40,0 - Obesidade Grave	III = 3

df_insarunce_01 = df_insarunce_01.withColumn(
    "imc_categoria",
    when((col('imc') >= 18.5) & (col('imc') <= 24.9),'0')
    .when((col('imc') >= 25.0) & (col('imc') <= 29.9),'1')
    .when((col('imc') >= 30.0) & (col('imc') <= 39.9),'2')
    .otherwise(3)
)

df_insarunce_01.select('imc_categoria').distinct().collect()

# COMMAND ----------


#Exibindo o dataframe tratado.
df_insarunce_01.display()

# COMMAND ----------

#Não sei se ficará
df_insarunce_01.printSchema()

# COMMAND ----------

from pyspark.sql.types import IntegerType
#conversando coluna string para o tipo integer

df_insarunce_01 = df_insarunce_01.withColumn('sexo', df_insarunce_01['sexo'].cast(IntegerType()))
df_insarunce_01 = df_insarunce_01.withColumn('imc_categoria', df_insarunce_01['imc_categoria'].cast(IntegerType()))
df_insarunce_01 = df_insarunce_01.withColumn('regiao', df_insarunce_01['regiao'].cast(IntegerType()))
df_insarunce_01 = df_insarunce_01.withColumn('fumante', df_insarunce_01['fumante'].cast(IntegerType()))

df_insarunce_01.printSchema()

# COMMAND ----------

# Transformando o dataframe em tabela sql para realizar consultas

df_insarunce_01.createOrReplaceGlobalTempView('seguro')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Analise explorátoria
# MAGIC
# MAGIC 1 - Qual é a maior frequencia de idade entre os clientes ?
# MAGIC
# MAGIC 2 - Qual a relação entre a quantidade de filhos e o imc ? 
# MAGIC
# MAGIC 3 - Qual é a media de idade de pessoas fumantes ?
# MAGIC
# MAGIC 4 - Qual é a região que mais possui clientes ? - ok
# MAGIC
# MAGIC 5 - Quantas pessoas são fumantes,possuem filho e estão acima do peso normal " Considerando a tabela imc" - ok
# MAGIC
# MAGIC 6 - Qual a media de despesas gasto por sexo ?
# MAGIC
# MAGIC 7 - Qual é maior idade do cliente crendenciado ?
# MAGIC

# COMMAND ----------

#4 - Qual é a região que mais possui clientes ?

spark.sql("""
          SELECT 
          CASE 
            when regiao = 0 then 'noroeste'
            when regiao = 1 then 'sudeste'
            when regiao = 2 then 'nordeste'
            when regiao = 3 then 'sudoeste'
            end as regiao,
          count(regiao) as quantidade
          FROM global_temp.seguro
          GROUP BY regiao
          ORDER BY quantidade desc
          """).show()


# COMMAND ----------

# Qual é o total de despesa gasto por região ?

spark.sql("""
            SELECT 
            CASE 
            when regiao = 0 then 'noroeste'
            when regiao = 1 then 'sudeste'
            when regiao = 2 then 'nordeste'
            when regiao = 3 then 'sudoeste'
            end as regiao,
            round(sum(Despesa),2) as Total_despesas
            FROM global_temp.seguro
            GROUP BY regiao
            order by Total_despesas asc

""").show()

# COMMAND ----------

#3 -Quais as idades que mais possuem fumantes ( top 5) ?
spark.sql("""
           
            SELECT idade ,count(idade) as quantidade_fumantes
            from global_temp.seguro
            where fumante = 1
            GROUP BY idade
            ORDER BY quantidade_fumantes desc
            limit 5
""").show()

# COMMAND ----------

#5 - Quantas pessoas são fumantes,possuem filho e estão acima do peso normal " Considerando a tabela imc"

spark.sql("""
           
            SELECT count(fumante) as qts_fumantes
            FROM global_temp.seguro
            where fumante > 0 and imc_categoria > 0 and filho > 0
            """).show()

# COMMAND ----------

#Qual é maior idade do cliente crendenciado ?
spark.sql("""

    SELECT max(idade)
    FROM global_temp.seguro
    
 """).show()

# COMMAND ----------

# Qual a media de despesas gasto por sexo ?

spark.sql(""" 
          select 
            case
                when sexo = 1 then 'Mulher'
                when sexo = 2 then 'Homen'
            end as sexo,
            round(avg(Despesa),2) as media_despesa
            FROM global_temp.seguro
            group by sexo
          
          """).show()
