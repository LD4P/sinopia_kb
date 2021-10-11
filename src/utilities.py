import json

import rdflib
import requests

from datetime import datetime
from typing import Dict, List, Optional


namespaces = {
    "bf": "http://id.loc.gov/ontologies/bibframe/",
    "bflc": "http://id.loc.gov/ontologies/bflc/",
    "mads": "http://www.loc.gov/mads/rdf/v1#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "sinopia": "http://sinopia.io/vocabulary/"
}

SINOPIA = rdflib.Namespace(namespaces.get('sinopia'))

def from_api(api_url: str) -> Dict:
    """Takes a Sinopia API endpoint URI, extracts each resource and
    template, and returns a dictionary with two lists, a resources and a
    templates, and the total number of resources harvested from the api.

    @param api_url -- URI to Sinopia API endpoint
    @param group -- optional Group name
    """

    def add_resource(resource):
        if not 'data' in resource:
            print(f"\n{resource.get('uri')} missing data")
            return
        output["total"] += 1
        graph = rdflib.Graph()
        for key, url in namespaces.items():
            graph.namespace_manager.bind(key, url)
        jsonld = json.dumps(resource.pop("data")).encode()
        try:
            graph.parse(data=jsonld, format="json-ld")
        except Exception as error:
            print(f"Failed to parse {resource}\n{error}")
            return
        payload = {"graph": graph, "meta": resource}
        if "sinopia:template:resource" in resource.get("templateId"):
            output["templates"].append(payload)
        else:
            output["resources"].append(payload)

    output = {"resources": [], "templates": [], "total": 0}
    start = datetime.utcnow()
    print(f"Started harvest of resources at {start} for {api_url}")
    initial = requests.get(f"{api_url}")
    print("0", end="")
    for row in initial.json().get("data"):
        add_resource(row)
    next_link = initial.json().get("links").get("next")
    while 1:
        result = requests.get(next_link)
        if result.status_code > 300:
            break
        payload = result.json()
        new_next = payload.get("links").get("next")
        if new_next is None:
            new_text = payload.get("links").get("first")
        if new_next == next_link or new_next is None:
            break
        for row in payload.get("data"):
            add_resource(row)
        next_link = new_next
        print(".", end="")
        if not output["total"] % 250:
            print(f"{output['total']}", end="")
    end = datetime.utcnow()
    print(f"\nFinished total time {(end-start).seconds / 60.}")
    return output

