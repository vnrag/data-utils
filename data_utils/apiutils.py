"""A library containing commonly used utils for general api usage
"""
import json
import urllib3


def handle_get_request(url, headers=None):
	"""Requests data from the provided URL via a get request

	Parameters
	----------
	url : String
		The URL to request data from

	Returns
	-------
	json
		Description

	Raises
	------
	SystemExit
		Description
	"""
	http = urllib3.PoolManager()
	r = http.request('GET', url, headers=headers)
	if r.status != 200:
		print(r.data)
		raise SystemExit
	return json.loads(r.data.decode('utf-8'))
