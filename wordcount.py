import wikipediaapi
import re
from collections import Counter
from pyspark.sql import SparkSession

def get_wikipedia_content(page_title, word_limit=10000):
    user_agent = "MyWikipediaBot/1.0 (Contact: sumanth12.sr@gmail.com)"  # Replace with your details
    wiki = wikipediaapi.Wikipedia(language="en", user_agent=user_agent)  # Corrected instantiation

    page = wiki.page(page_title)

    if not page.exists():
        print("Page '{}' does not exist.".format(page_title))
        return None

    # Extract words, normalize to lowercase, and limit to 10,000 words
    words = re.findall(r'\b\w+\b', page.text.lower())[:word_limit]
    return words

def process_and_save_rdd(words, output_path="word_counts"):
    
    # Initialize Spark Session
    spark = SparkSession.builder.appName("WikipediaWordCount").getOrCreate()

    # Convert word list to an RDD
    rdd = spark.sparkContext.parallelize(words)
    

    # Map each word to (word, 1) pairs and reduce by key (word count)
    word_counts_rdd = rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    # Save the RDD to the specified output path
    word_counts_rdd.coalesce(1).saveAsTextFile(output_path)

    # Print total word count
    total_words = word_counts_rdd.map(lambda x: x[1]).sum()
    print("\nTotal words counted (including repetitions):", total_words)

    spark.stop()

# def group_words(words):
    
#     return Counter(words)  # Count occurrences of each word

# def save_to_file(word_counts, filename="word_counts.txt"):
#     with open(filename, "w", encoding="utf-8") as file:
#         for word, count in sorted(word_counts.items(), key=lambda x: x[1], reverse=True):
#             file.write("{}: {}\n".format(word, count))
#     print("Word count saved to {}".format(filename))


if __name__ == "__main__":
    page_title = "Artificial Intelligence"  # Change this to any Wikipedia page title
    words = get_wikipedia_content(page_title)

    if words:
        # word_counts = group_words(words)
        # save_to_file(word_counts)
        process_and_save_rdd(words)