from pyspark import SparkContext, SparkConf

# --- Configuracion de Spark ---
conf = SparkConf().setAppName("WordCount_UNAD").setMaster("local[*]")
sc = SparkContext(conf=conf)

# --- Cargar archivo de texto ---
text_file = sc.textFile("file:///media/sf_Guia_3/Aplicacion.txt")

# --- Procesamiento: contar palabras ---
word_counts = (
    text_file.flatMap(lambda line: line.lower().split())
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)

# --- Guardar resultados en HTML ---
sorted_counts = word_counts.sortBy(lambda x: x[1], ascending=False).collect()

with open("/media/sf_Guia_3/resultado_wordcount.html", "w") as f:
    f.write("<html><head><title>Resultado WordCount</title></head><body>")
    f.write("<h2>Conteo de palabras (PySpark - UNAD)</h2>")
    f.write("<table border='1'><tr><th>Palabra</th><th>Frecuencia</th></tr>")
    for word, count in sorted_counts:
        f.write(f"<tr><td>{word}</td><td>{count}</td></tr>")
    f.write("</table></body></html>")

print("✅ Resultado generado en: /media/sf_Guia_3/resultado_wordcount.html")

sc.stop()
