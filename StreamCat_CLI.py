import click
import os
import pandas as pd
import numpy as np
from StreamCat import process_metric
from config_tables.stream_cat_config import ConfigArgs
from functions import NHDProcessing
from database import DatabaseConnection
# TODO

@click.group()
def cli():
    """Creating click group for cli commands and args
    """
    pass 

@click.command()
@click.option('--use_arcpy', type=click.BOOL, default=True, help='Wheter or not using an arcpy environments')
@click.option('--num_workers', type=click.STRING, default=os.cpu_count(), help='Number of processes/workers to use for parallelized functions throughout StreamCat pipeline.')
@click.option('--control_table', type=click.Path(exists=True), default='./ControlTable_StreamCat.csv', help='Path to control table csv file')
@click.option('--write_to_db', type=click.BOOL, default=False, help='If True we connect to DB and write new metrics to database directly. Else we skip this.')
def StreamCat_CLI(use_arcpy, num_workers, control_table, write_to_db):
    click.echo("-- Welcome to the StreamCat CLI --")
    args = ConfigArgs()
    ctl = pd.read_csv(control_table)
    # Load table of inter vpu connections
    inter_vpu = pd.read_csv(args.inter_vpu)

    if not os.path.exists(args.OUT_DIR):
        os.mkdir(args.OUT_DIR)

    if not os.path.exists(args.OUT_DIR + "/DBF_stash"):
        os.mkdir(args.OUT_DIR + "/DBF_stash")

    if not os.path.exists(args.ACCUM_DIR):
        # TODO: work out children OR bastards only
        nhd = NHDProcessing(args.NHD_DIR)
        nhd.makeNumpyVectors(inter_vpu, args.NHD_DIR, args.USER_ZONES)

    INPUTS = np.load(args.ACCUM_DIR +"/vpu_inputs.npy", allow_pickle=True).item()

    if write_to_db:
        db_conn = DatabaseConnection()
        db_conn.connect()

    with click.progressbar(iterable=ctl.query("run == 1").iterrows(), label="Processing Metrics") as bar:
        for row in bar:
            # TODO alter process metric to take in the other args (use_arcpy, num_workers) as params
            process_metric(args, row, inter_vpu, INPUTS)

if __name__ == '__main__':
    results = StreamCat_CLI()