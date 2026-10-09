from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName("ProcesamientoBatchGuia") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n--- 1. CARGA DEL DATASET ---")
df = spark.read.csv("datos_batch.csv", header=True, inferSchema=True)
df.show()

print("Esquema original:")
df.printSchema()

print("\n--- 2. LIMPIEZA DE DATOS ---")
df_clean = df.dropna(subset=["categoria", "valor"])
print(f"Filas originales: {df.count()} | Filas tras limpieza: {df_clean.count()}")

print("\n--- 3. ANALISIS EXPLORATORIO DE DATOS (EDA) ---")
print("Estadisticas descriptivas:")
df_clean.describe("valor").show()

print("Agregacion por categoria:")
df_resumen = df_clean.groupBy("categoria").agg(
    count("id").alias("total_ventas"),
    avg("valor").alias("promedio_valor"),
    sum("valor").alias("total_monto")
)

df_resumen.show()

print("--- 4. VISUALIZACION DE RESULTADOS ---")
filas = df_resumen.collect()
categorias = [row["categoria"] for row in filas]
montos = [row["total_monto"] for row in filas]

plt.figure(figsize=(8, 5))
plt.bar(categorias, montos, color=['#1f77b4', '#ff7f0e', '#2ca02c'])
plt.title("Monto Total de Ventas por categoria (Spark Batch)")
plt.xlabel("Categoria")
plt.ylabel("Monto Total ($)")
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.savefig("grafico_batch.png")
print(" Grafica guardada exitosamente como 'grafico_batch.png'")

spark.stop()
