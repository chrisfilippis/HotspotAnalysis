from sklearn.cluster import KMeans
import numpy as np

X = np.array([[1, 1], [1, 0], [0, 2], [2, 4], [3, 5]])
k_means = KMeans(n_clusters=2, random_state=2).fit(X)

print "k_means"
print k_means.labels_
print k_means.cluster_centers_
