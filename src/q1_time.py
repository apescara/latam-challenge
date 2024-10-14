from typing import List, Tuple
from datetime import datetime
import json
import pandas as pd
import pendulum


def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:

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

    # Arreglar datos a utilizar
    data["date_part"] = data.date.apply(lambda x: pendulum.parse(x).date())
    data["username"] = data.user.apply(lambda x: x["username"])

    # Seleccionar y agrupar data a usar
    data_group = (
        data[["date_part", "username", "id"]]
        .groupby(["date_part", "username"])
        .count()
        .reset_index()
        .sort_values(by="id", ascending=False)
    )

    # Calcular resultados, se obtendra por un lado los dias mas activos y por el otro los usuarios mas activos por cada día.
    # Finalmente se realizara el cruce de los 2
    most_active_dates = (
        data_group.groupby("date_part")["id"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .index
    )
    most_active_user_by_date = (
        data_group.groupby(["date_part", "username"])["id"]
        .sum()
        .sort_values(ascending=False)
        .groupby(level=0)
        .idxmax()
        .apply(lambda x: x[1])
    )
    most_active_dates_with_most_active_users = most_active_user_by_date.loc[
        most_active_dates
    ]

    # Dar formato final
    out = []
    for index, row in most_active_dates_with_most_active_users.reset_index().iterrows():
        out.append((row.iloc[0], row.iloc[1]))

    return out
