import json

from pandas import DataFrame


def dump_data_redis(data: DataFrame, redis_uri):
    """
    Function to update user-cluster relations in
    redis cache
    :param data: Dataframe object pandas
    :param redis_uri: redis uri
    """
    data_dict = data.to_dict(orient='records')
    cls = Redis.from_uri(redis_uri)
    for record in data_dict:
        key = "user_information:{}".format(record[CUSTOMER_ID])
        record.pop(CUSTOMER_ID, None)
        cls.set(key, json.dumps(record))
    return True

if __name__ == '__main__':
    redis_uri = 'redis://:@dev-redis-7c2ce062b75a23c2.elb.ap-southeast-1.amazonaws.com:6397/1'
    existing_cluster = UpdaterUtils.fetch_existing_cluster(bucket_name=BUCKET_NAME,
                                                           object_name=cluster_pkl_path_file)
    existing_cluster[CUSTOMER_ID] = existing_cluster[CUSTOMER_ID].astype(str)
    customer_dummy = pd.read_csv(
        '/Users/nishantsingh/PycharmProjects/rce_offline_results_module/offline_results/updater/redis_dump_customer.csv'
    )
    customer_dummy[CUSTOMER_ID] = customer_dummy[CUSTOMER_ID].astype(str)
    existing_cluster = existing_cluster[existing_cluster[CUSTOMER_ID].isin(customer_dummy[CUSTOMER_ID])]
    # existing_cluster = existing_cluster.drop(columns=[CLUSTER_IS_PAY_TV])
    # tmp_clusters = pd.read_pickle('tmp_clusters.pkl', compression='gzip')
    dump_data_redis(data=existing_cluster, redis_uri=redis_uri)
