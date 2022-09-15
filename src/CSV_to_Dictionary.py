import csv
from pyspark.sql import SparkSession


def main():
    # Setup pyspark
    spark_session = SparkSession.builder.master("local[1]") \
        .appName('CSV_to_Dictionary') \
        .getOrCreate()

    spark = spark_session.sparkContext

    # read all the csv file
    file_path = ""
    name = []
    content = []
    final_Dict = []
    with open(file_path, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        name = csv_reader[0]
        content = csv_reader[1:]

    # convert CSV to Dictionary
    def convert(keys, values):
        result = {}
        for x in range(len(keys)):
            result.update({keys[x]: values[x]})

    final_Dict = spark.parallelize(content).\
        map(lambda x: convert(name, x)).\
        collect()

    # TODO: Store dictionary in somewhere
    return


if __name__ == '__main__':
    main()
