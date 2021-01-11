from typing import List, Union

from pydantic import BaseModel


class Text(BaseModel):
    location: Union[str, List[str]]
