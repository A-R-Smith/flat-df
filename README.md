## Flat Dataframe

This is a framework for denormalizing nested XML files using the databricks spark XML parser. The main algorithm is located in the FlatDataFrame.flatten method.


# Limitations

Right now the flatten algorithm can only handle single nested arrays. If there is an array within an array, the algorithm does not denormalize that.
