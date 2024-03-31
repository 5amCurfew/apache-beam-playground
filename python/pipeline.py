import argparse
import datetime
import logging
from typing import Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# Transform each element into a tuple where the first field is userId and assigns the timestamp to 
# the metadata of the element such that window functions can use it later to group elements into windows.
class TransformDoFn(beam.DoFn):
    def process(self, element):
        unix_timestamp = element["recordedAt"]
        # Each element of the pipeline is transformed into a tuple where the first field is the userId 
        # (which will be the assumed key in CombinePerKey)
        element = (
            element["userId"], 
            1 if element["event"] == "click" else 0
        )
        yield beam.transforms.window.TimestampedValue(element, unix_timestamp)

# Pluck Session information from the metadata of the element
class SessionSummaryDoFn(beam.DoFn):
    def process(self, aggregated_element, window=beam.DoFn.WindowParam):
        session_id = {
            "userId": aggregated_element[0],
            "sessionStartedAt": window.start.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S"), 
            "sessionEndedAt": window.end.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S")
        }
        yield (session_id, {"total": aggregated_element[1]})

def build_run(pipeline_options: Optional[PipelineOptions] = None, known_args: Optional[dict] = None, other_args: Optional[list] = None):
    with beam.Pipeline(options=pipeline_options) as p:

        if known_args.new_user is None:
            logging.info('No --new-user provided. Using default value None')
        (
            p 
            | "Create" >> beam.Create(
                [
                    {"userId": "Andy", "event": "click", "recordedAt": 1603112520},     # Monday, 19 October 2020 13:02:00
                    {"userId": "Sam", "event": "click", "recordedAt": 1603113240},      # Monday, 19 October 2020 13:14:00
                    {"userId": "Sam", "event": "screen", "recordedAt": 1603113244},     # Monday, 19 October 2020 13:14:04
                    {"userId": "Sam", "event": "screen", "recordedAt": 1603113248},     # Monday, 19 October 2020 13:14:08
                    {"userId": "Sam", "event": "screen", "recordedAt": 1603113249},     # Monday, 19 October 2020 13:14:09
                    {"userId": "Andy", "event": "click", "recordedAt": 1603113600},     # Monday, 19 October 2020 13:20:00
                    {"userId": "Andy", "event": "click", "recordedAt": 1603115820},     # Monday, 19 October 2020 13:57:00
                    {"userId": known_args.new_user, "event": "click", "recordedAt": int(datetime.datetime.now().strftime('%s'))},  # "Now"
                ]
            )
            # Assign timestamp to metadata of elements such that Beam's window functions can
            # access and use them to group events.
            # | "Filter" >> beam.ParDo(FilterDoFn())
            | "Transform: TimestampedValue" >> beam.ParDo(TransformDoFn())
            | "Assign Session Window" >> beam.WindowInto(
                # Each session must be separated by a time gap of at least 30 minutes (1800 sec)
                beam.transforms.window.Sessions(gap_size=30 * 60), # This is a class extended from WindowFn

                # Triggers determine when to emit the aggregated results of each window. Default
                # trigger outputs the aggregated result when it estimates all data has arrived,
                # and discards all subsequent data for that window.
                # https://medium.com/@shlomisderot/apache-beam-windows-late-data-and-triggers-e2e856c502b9
                trigger=None,

                # Since a trigger can fire multiple times, the accumulation mode determines
                # whether the system accumulates the window panes as the trigger fires, or
                # discards them.
                accumulation_mode=None,

                # Policies for combining timestamps that occur within a window. Only relevant if
                # a grouping operation is applied to windows.
                timestamp_combiner=None,

                # By setting allowed_lateness we can handle late data. If allowed lateness is
                # set, the default trigger will emit new results immediately whenever late
                # data arrives.
                allowed_lateness=beam.transforms.window.Duration(seconds=300),  # 5 Minutes
            )
            # | "Count By Key,Window" >> beam.combiners.Count.PerKey()    
            | "Sum Clicks By Key,Window" >> beam.CombinePerKey(sum)
            | "Transform: Session Summary" >> beam.ParDo(SessionSummaryDoFn())
            | "Print" >> beam.ParDo(print)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--new-user", default=None, help="Parse Arguments using argparse")
    known_args, other_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        runner="DirectRunner",
        streaming=True,
        save_main_session=True, 
        setup_file="./setup.py"
    )

    build_run(
        pipeline_options=pipeline_options,
        known_args=known_args,
        other_args=other_args,
    )