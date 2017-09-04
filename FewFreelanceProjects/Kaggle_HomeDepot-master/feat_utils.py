def try_divide(x, y, val=0.0):
    """ 
    	Try to divide two numbers
    """
    if y != 0.0:
    	val = float(x) / y
    return val


def get_sample_indices_by_relevance(dfTrain, additional_key=None):
	""" 
		return a dict with
		key: (additional_key, median_relevance)
		val: list of sample indices
	"""
	dfTrain["sample_index"] = range(dfTrain.shape[0])
	group_key = ["median_relevance"]
	if additional_key != None:
		group_key.insert(0, additional_key)
	agg = dfTrain.groupby(group_key, as_index=False).apply(lambda x: list(x["sample_index"]))
	d = dict(agg)
	dfTrain = dfTrain.drop("sample_index", axis=1)
	return d
