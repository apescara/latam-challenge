from typing import List, Tuple

import pandas as pd
import json 

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    result = dict()
    with open(file_path, "r") as f:
        # En el archivo hay un json por linea, hay que itearr y extrar cada uno
        for line in f:
            data = pd.json_normalize(json.loads(line))
            if "quotedTweet.mentionedUsers" in data.columns: # Hay registros que no tienen la columna
                if data["quotedTweet.mentionedUsers"][0]: # Y otros que vienen con "None"
                    for u in data["quotedTweet.mentionedUsers"][0]:
                        result[u["username"]] = result.get(u["username"], 0) + 1
    
    top_mentions = pd.json_normalize(result).T.reset_index().sort_values(0,ascending=False).head(10)
        
    out = []
    for index, row in top_mentions.iterrows():
        out.append((row.loc["index"], row.loc[0]))

    return out