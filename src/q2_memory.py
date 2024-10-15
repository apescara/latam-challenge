from typing import List, Tuple

import emoji
import json 
import pandas as pd

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    result = dict()
    with open(file_path, "r") as f:
        # En el archivo hay un json por linea, hay que itearr y extrar cada uno
        for line in f:
            data = pd.json_normalize(json.loads(line))
            for c in data.content.to_list():
                for e in emoji.emoji_list(c):
                    result[e["emoji"]] = result.get(e["emoji"], 0) + 1


    top_emojis = pd.json_normalize(result).T.reset_index().sort_values(0,ascending=False).head(10)
        
    out = []
    for index, row in top_emojis.iterrows():
        out.append((row.loc["index"], row.loc[0]))

    return out