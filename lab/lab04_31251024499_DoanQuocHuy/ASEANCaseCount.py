from pyspark import SparkContext

def clean_number(value):
    try:
        return int(float(value.replace(',', '')))
    except:
        return 0

def main():
    sc = SparkContext(appName="ASEAN Cumulative Case Count RDD")

    raw_data = \
        sc.textFile("hdfs://namenode:9000/user/doanquochuy/lab04/input/" \
        "WHO-COVID-19-20210601-213841.tsv")
    header = raw_data.first()
    data_no_header = raw_data.filter(lambda line: line != header)

    asean_cases = data_no_header \
        .map(lambda line: line.split('\t')) \
        .filter(lambda row: len(row) > 2 and row[1].strip() == "South-East Asia") \
        .map(lambda row: ("South-East Asia Total Cases", clean_number(row[2]))) \
        .reduceByKey(lambda a, b: a + b)

    result = asean_cases.collect()
    for key, value in result:
        print(f"{key}: {value}")

    sc.stop()

main()
