from concurrent.futures import ThreadPoolExecutor

# if max_workers is not specified, then it uses default, which is (#processors) * 5
glob_executor = ThreadPoolExecutor(max_workers=1000)