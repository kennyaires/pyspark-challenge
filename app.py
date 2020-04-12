from pyspark import SparkConf, SparkContext
from operator import add
from datetime import datetime


# Initialize SparkContext
conf = SparkConf().setAppName("PySpark App").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Upload files into spark context and join RDD's
"""
    HTTP requests to NASA Kennedy Space Center in two given periods
    Line example: "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245"
    Respectively: "{host} - - [{timestamp}] "{url details}" {status response} {bytes sent}"

    Data available on:
    ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
    ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
"""
access_log = sc.textFile('access_log_Jul95')
access_log_2 = sc.textFile('access_log_Aug95')
access_log = access_log.union(access_log_2).cache()


def unique_hosts_count(rdd):
    """ 
        Return the number of unique origin hosts

        return type: int                         
    """

    unique_hosts = rdd.map(lambda line: line.split(' ')[0])

    return unique_hosts.distinct().count()


def get_all_with_given_response(rdd, response='404'):
    """ 
        Return a rdd only with those requests 
        that received the response code entered.
        Default set to '404'.

        return type: pyspark.rdd.PipelinedRDD
    """

    def status_iterator(ln):
        try:
            status = ln.split(' ')[-2]
            return True if status == response else False
        except:
            pass

    return rdd.filter(status_iterator)


def top_five_urls(rdd):
    """ 
        Return a rdd only with the top 5 url's
        with more requests by descending order.

        return type: pyspark.rdd.PipelinedRDD
    """

    urls_rdd = rdd.map(lambda line: line.split('"')[1].split(' ')[1])
    count_rdd = urls_rdd.map(lambda url: (url, 1)).reduceByKey(add)

    return count_rdd.sortBy(lambda el: -el[1]).take(5)


def get_requests_daily(rdd):
    """
        Return a rdd with the total number of
        requests per day.

        return type: list
    """

    days_rdd = rdd.map(lambda line: line.split('[')[1].split(':')[0])
    count_rdd = days_rdd.map(lambda day: (datetime.strptime(day, "%d/%b/%Y"), 1))\
        .reduceByKey(add)
    return count_rdd.sortBy(lambda el: el[0]).collect()


def sum_all_bytes(rdd):
    """
        Return the sum of all bytes sent

        return type: int
    """

    def bytes_iterator(ln):
        try:
            bytes_val = int(ln.split(' ')[-1])
            return bytes_val if bytes_val > 0 else 0
        except:
            return 0

    return rdd.map(bytes_iterator).reduce(add)


if __name__ == "__main__":
    """ Usage of f-Strings available only on python 3.6 versions or above  """
    print('\n*** RESULTS ***')

    # 1. Number of unique hosts
    print('\n#1. Number of unique hosts:', f'{unique_hosts_count(access_log)} hosts')

    # 2. Total of 404 errors
    rdd_of_all_404_responses = get_all_with_given_response(access_log).cache()
    print('\n#2. Total of 404 errors:', rdd_of_all_404_responses.count())

    # 3. The top 5 most frequent URL's with 404 error
    top_five_urls_with_more_errors = top_five_urls(rdd_of_all_404_responses)
    print("\n#3. The top 5 most frequent URL's with 404 errors:")
    print('\n'.join(f'  {idx+1}. | "{item[0]}" | {item[1]} responses' \
            for idx, item in enumerate(top_five_urls_with_more_errors)))

    # 4. Number of 404 errors daily
    print("\n#4. Number of 404 errors daily:")
    daily_404_responses = get_requests_daily(rdd_of_all_404_responses)
    print('\n'.join(f' - {(item[0]).strftime("%d/%m/%Y")}: {item[1]} error responses' \
            for item in daily_404_responses))

    # 5. Total number of bytes sent
    print("\n#5. Total number of bytes sent:", f"{sum_all_bytes(access_log)} bytes")

    print('\n*** END ***')

    sc.stop()