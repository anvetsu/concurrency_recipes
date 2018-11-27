"""

Concurrent requests using requests and concurrent futures.

"""

import requests
import time
import multiprocessing as mp

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

class ParallelFetcher:
    """ A Parallel URL fetcher class using concurrent futures """

    # This class provides two top-level methods. You
    # can either use `fetch_all` or use `wait(submit(...))`
    # The former waits for all results in one go and the
    # latter is asynchronous, processing results as they
    # come.
    
    def __init__(self, urls=[], timeout=30):
        self.urls = urls
        self.timeout = timeout
        
    def fetch_url(self, url, timeout=30):
        """ Fetch a given URL using requests """
        
        if not url.startswith('http'):
            url = 'http://' + url

        try:
            print('Fetching',url)
            response = requests.get(url, timeout=timeout)
            return response.url, (response.status_code, response.content)
        except Exception as e:
            print('Error fetching URL =>', e)
            
    def fetch_all(self, use_threads=False):
        """ Fetch all URLs parallely """

        # Use threadpool executor
        if use_threads:
            executor = ThreadPoolExecutor
        else:
            executor = ProcessPoolExecutor
            
        with executor(max_workers=mp.cpu_count()) as executor:
            results = executor.map(self.fetch_url, self.urls)

        # Map it as a dict
        return dict(results)

    def submit(self, use_threads=False):
        """ Submit an asynchronous request for fetching URLs and
        return a futures map """
        
        if use_threads:
            executor = ThreadPoolExecutor
        else:
            executor = ProcessPoolExecutor
            
        pexec = executor(max_workers = mp.cpu_count())

        # Return a generator expression
        return (pexec.submit(self.fetch_url, url) for url in urls)

    def wait(self, futures):
        """ Wait on the futures iterator and return results """
        
        for future in as_completed(futures):
            status = future.result()
            if status:
                url = status[0]
                print('URL',url,'downloaded, HTTP code =>',status[1][0])
                yield status

if __name__ == "__main__":
    urls = ['http://www.anvetsu.com','http://kubernetes.com','http://opendns.com',
            'http://docs.python-request.org']

    pfetch = ParallelFetcher(urls = urls)
    options = (False, True)
    
    for option in options:
        if option:
            print('#####Using Thread Pool#####')
        else:
            print('#####Using Process Pool#####')           
        
        print('--------Using map--------')  
        t1 = time.time()
        results= pfetch.fetch_all(use_threads=option)
        
        for url in results:
            res = results[url]
            print (url,'=>',res[0])
            
        t2 = time.time()
        print('Time taken #1 =>',t2-t1)
        
        print('--------Using submit and wait--------')
        # 2nd method is slightly more efficient because of the
        # async nature
        t1 = time.time()    
        results = pfetch.wait(pfetch.submit(use_threads=option))
        
        for url,status in results:
            print (url,'=>',status[0])     
            
        t2 = time.time()
        print('Time taken #2 =>',t2-t1)
        

