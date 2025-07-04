# Databricks notebook source
# MAGIC %md
# MAGIC ### WORD COUNT PROBLEMS INGESTION

# COMMAND ----------

# MAGIC %md
# MAGIC #### NOW USE IN SINGLE FUNCTION PYSPARK FUNCTION

# COMMAND ----------

# Databricks notebook source
class batchWC():
    def __init__(self, base_data_dir=None):
        # Default to your datasets folder if no path is passed
        self.base_data_dir = base_data_dir or "dbfs:/FileStore/shared_uploads/himanshu.kumar1@tothenew.com/datasets"

    def getRawData(self):
        from pyspark.sql.functions import explode, split
        lines = (
            spark.read
            .format("text")
            .option("lineSep", ".")  # Optional: depends on your file format
            .load(self.base_data_dir)
        )
        return lines.select(explode(split(lines.value, " ")).alias("word"))

    def getQualityData(self, rawDF):
        from pyspark.sql.functions import trim, lower
        return (
            rawDF.select(lower(trim(rawDF.word)).alias("word"))
                 .where("word IS NOT NULL AND word RLIKE '[a-z]'")
        )

    def getWordCount(self, qualityDF):
        return qualityDF.groupBy("word").count()

    def appendWordCount(self, wordCountDF):
        # ✅ Use append mode for cumulative result
        wordCountDF.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("word_count_table")

    def wordCount(self):
        print(f"\tExecuting Word Count...", end='')
        rawDF = self.getRawData()
        qualityDF = self.getQualityData(rawDF)
        resultDF = self.getWordCount(qualityDF)
        self.appendWordCount(resultDF)
        print("Done")

        # Optional: Show top 10 words
        resultDF.orderBy("count", ascending=False).show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ### STREAMING DATA
# MAGIC

# COMMAND ----------

# Databricks notebook source
class streamWC():
    def __init__(self, base_data_dir=None):
        # Default path if not provided externally
        self.base_data_dir = base_data_dir or "dbfs:/FileStore/shared_uploads/himanshu.kumar1@tothenew.com/data/text"

    def getRawData(self):
        from pyspark.sql.functions import explode, split
        lines = (
            spark.readStream
            .format("text")
            .option("lineSep", ".")  # Optional depending on your data
            .load(self.base_data_dir)
        )
        return lines.select(explode(split(lines.value, " ")).alias("word"))

    def getQualityData(self, rawDF):
        from pyspark.sql.functions import trim, lower
        return (
            rawDF.select(lower(trim(rawDF.word)).alias("word"))
                 .where("word IS NOT NULL AND word RLIKE '[a-z]'")
        )

    def getWordCount(self, qualityDF):
        return qualityDF.groupBy("word").count()

    def overwriteWordCount(self, wordCountDF):
        # ✅ Trigger only once after ingestion
        return wordCountDF.writeStream \
            .format("delta") \
            .outputMode("complete") \
            .trigger(once=True) \
            .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/word_count") \
            .toTable("word_count_table")


    def wordCount(self):
        print(f"\tStarting Word Count Stream...", end='')
        rawDF = self.getRawData()
        qualityDF = self.getQualityData(rawDF)
        resultDF = self.getWordCount(qualityDF)
        sQuery = self.overwriteWordCount(resultDF)
        print("Done")
        return sQuery


# COMMAND ----------

