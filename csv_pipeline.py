import apache_beam as beam
from apache_beam.io.textio import ReadFromText, WriteToText
from geopy.distance import distance
import typing

class Journey(typing.NamedTuple):
    start_hub:  int
    end_hub:    int
    distance:   float

def process_trip(trip):
    (x_str, count) = trip
    x = x_str.split(',')
    j_dist = distance((x[2], x[1]), (x[5], x[4])).km
    j = Journey(start_hub=int(x[0]), end_hub=int(x[3]), distance=j_dist)
    return (j, count)

def format_trips(trip):
    (j, count) = trip
    return f"{j.start_hub}, {j.end_hub}, {count}, {j.distance * count:.2f}"

with beam.Pipeline() as pipeline:
   ride_counts = (
       pipeline
       | 'Ingest rows'     >> ReadFromText("data_220630.csv", skip_header_lines=1)
       | 'Make key + 1'    >> beam.Map(lambda x: (x, 1))
       | 'Aggregate rows'  >> beam.CombinePerKey(sum)
       | 'Create journeys' >> beam.Map(process_trip)
       )
   
   output = ride_counts | 'Format count' >> beam.Map(format_trips)
   output | WriteToText("output.csv")
  
  