from joblib import Parallel, delayed


class parallelizer():
    def __init__(self,**kwargs):
        self.n_jobs=kwargs.get('n_jobs', 1)

    def result_concat(self,results):
        return results

    def parallelize_df(self, parrall_meter, axis=0, start=0):
        def decorator(func):
            def wrapper(*args,**kwargs):
                n_jobs = self.n_jobs
                if axis == 0:
                    data_count=(kwargs[parrall_meter].shape[0]-start)
                    chunk_size =  data_count// n_jobs
                    chunks=[]
                    for i in range(0, data_count, chunk_size):
                        sub_kwargs={k:v.iloc[list(range(0,start))+list(range(start+i,min(start+i+chunk_size,data_count))),:] for k,v in kwargs.items() if k==parrall_meter}
                        sub_kwargs.update({k:v for k,v in kwargs.items() if k!=parrall_meter})
                        chunks.append(sub_kwargs)

                elif axis == 1:
                    
                    data_count=(kwargs[parrall_meter].shape[1]-start)
                    chunk_size =  data_count// n_jobs
                    chunks=[]
                    for i in range(0, data_count, chunk_size):
                        sub_kwargs={k:v.iloc[:,list(range(0,start))+list(range(start+i,min(start+i+chunk_size,data_count)))] for k,v in kwargs.items() if k==parrall_meter}
                        sub_kwargs.update({k:v for k,v in kwargs.items() if k!=parrall_meter})
                        chunks.append(sub_kwargs)
                else:
                    raise ValueError("Invalid axis value. Must be 0 or 1.")
                results = Parallel(n_jobs=n_jobs,backend="loky")(delayed(func)(*args,**chunk) for chunk in chunks)
                return self.result_concat(results)
            return wrapper
        return decorator

