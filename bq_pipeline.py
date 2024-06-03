import apache_beam as beam
from apache_beam.io.textio import ReadFromText, WriteToText
from geopy.distance import distance
import typing
import re
import glob

class Journey(typing.NamedTuple):
    start_hub:  int
    end_hub:    int
    distance:   float

def process_trip(trip):
    (x_str, count) = trip
    x = x_str.split(',')
    j_dist = distance((x[2], x[1]), (x[5], x[4])).km
    j = f'{x[0]}-{x[3]}-{j_dist:.4f}'
    return (j, count)

def process_agg(agg):
    x = re.sub("[\(\) \']", '', agg)
    x = x.split(',')
    return (x[0], int(x[1]))

def ingest_journey(s):
    j = s.split('-')
    return Journey(start_hub=int(j[0]), end_hub=int(j[1]), distance=float(j[2]))

def process_cgbk(cgbk):
    (s, d) = cgbk
    j = ingest_journey(s)
    t = 0
    for v in d.values():
        t += sum(v)
    return (j, t)

def format_trips(trip):
    (j, count) = trip
    return f"{j.start_hub}, {j.end_hub}, {count}, {j.distance * count:.2f}"

for i in range(1,101):
    with beam.Pipeline() as pipeline:
       (
            pipeline
            | f'{i} - Ingest rows'     >> ReadFromText(f"tables/export_{i}.csv")
            | f'{i} - Make key + 1'    >> beam.Map(lambda x: (x, 1))
            | f'{i} - Aggregate rows'  >> beam.CombinePerKey(sum)
            | f'{i} - Create journeys' >> beam.Map(process_trip)
            | f'{i} - Output agg'      >> WriteToText(f"aggs/agg_{i}.csv")
        )

files = glob.glob('aggs/*')
agg_dict = {}

with beam.Pipeline() as pipeline:
    for i in range(len(files)):
        agg_dict[f'a{i}'] = (
            pipeline
            | f'{i} - Read' >> ReadFromText(files[i])
            | f'{i} - Process Agg' >> beam.Map(process_agg)
        )

    combo_agg = (
        (agg_dict)
     | beam.CoGroupByKey()
     | beam.Map(process_cgbk)
     | beam.Map(format_trips)
     | 'Output final' >> WriteToText("output/output.csv")
     )