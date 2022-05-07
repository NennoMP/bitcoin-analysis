import pandas as pd
from data_validator import DataValidator
from data_analyzer import DataAnalyzer

# Non-validated data-set files
TXS_FILE = r'data/transactions.csv'
INPUTS_FILE = r'data/inputs.csv'
OUTPUTS_FILE = r'data/outputs.csv'

# Validated data-set files
VALIDATED_TXS_FILE = r'data/validated_transactions.csv'
VALIDATED_INPUTS_FILE = r'data/validated_inputs.csv'
VALIDATED_OUTPUTS_FILE = r'data/validated_outputs.csv'


def load_data(txf: str, inf: str, outf: str) -> pd.DataFrame:
    """Load a .csv data-set into a pandas.Dataframe."""

    # Load Transactions
    tx_data = pd.read_csv(txf)
    tx_df = pd.DataFrame(tx_data)
    tx_df.drop_duplicates(subset='id', keep='last', inplace=True)

    # Load Inputs
    in_data = pd.read_csv(inf)
    in_df = pd.DataFrame(in_data)

    # Load Outputs
    out_data = pd.read_csv(outf)
    out_df = pd.DataFrame(out_data)

    return tx_df, in_df, out_df   


def main():

    # Validation
    #tx_df, in_df, out_df = load_data(TXS_FILE, INPUTS_FILE, OUTPUTS_FILE)
    #dv = DataValidator(tx_df, in_df, out_df)
    #dv.validate_data()

    # Load validated dataset
    tx_df, in_df, out_df = load_data(VALIDATED_TXS_FILE, VALIDATED_INPUTS_FILE, VALIDATED_OUTPUTS_FILE)

    # Perform analysis
    da = DataAnalyzer(tx_df, in_df, out_df)
    da.get_utxo()
    da.block_occupancy_analytics()
    da.received_btc_analytics()
    da.fees_analytics()

if __name__ == '__main__':
    main()