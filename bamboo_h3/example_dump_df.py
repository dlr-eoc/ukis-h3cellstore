
import bamboo_h3
import pandas as pd
import numpy as np



df = pd.DataFrame({
    'a': np.zeros(10, dtype='uint8'),
    'b': np.full(10, 34, dtype='uint32')
})

bamboo_h3.dump_dataframe(df)
