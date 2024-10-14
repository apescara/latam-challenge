from typing import List, Tuple

import emoji
import json 
import pandas as pd

def q2_time(file_path: str) -> List[Tuple[str, int]]:


    def read_json(file_path):
        data = []
        with open(file_path, "r") as f:
            # En el archivo hay un json por linea, hay que itearr y extrar cada uno
            for line in f:
                data.append(json.loads(line))
            f.close()

        return data

    # Leer json y pasar a DF
    data = read_json(file_path=file_path)
    data = pd.DataFrame.from_records(data)

    # Identifico los emojis, los separo y extraigo los 10 mas usados
    emoji_usage = data.content.apply(lambda x: emoji.emoji_list(x)).to_frame()
    concatenation = emoji_usage[emoji_usage.content.str.len() != 0].content.sum()
    top_emojis = (
        pd.DataFrame.from_records(concatenation)[["emoji", "match_start"]]
        .groupby("emoji")
        .count()
        .reset_index()
        .sort_values(by="match_start", ascending=False)
        .head(10)
    )
        
    out = []
    for index, row in top_emojis.reset_index().iterrows():
        out.append((row.iloc[1], row.iloc[2]))

    return out