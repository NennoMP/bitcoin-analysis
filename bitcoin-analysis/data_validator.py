import json
import pandas as pd

# Validation results files
VALIDATED_FOLDER = r'bitcoin-analysis/data/'
VALIDATION_FILE = r'bitcoin-analysis/data/analytics/validation_analytics.json'
UTXO_FILE = r'bitcoin-analysis/data/analytics/utxo_ids.txt'

# Constants
MINING_REWARD = 5_000_000_000 # (Satoshi)


class DataValidator:

    # Dataframes
    tx_df: pd.DataFrame
    in_df: pd.DataFrame
    out_df: pd.DataFrame

    # Analytics data
    n_invalid: int = 0
    utxo: set = set() # current unspent outputs ids         
    not_in_utxo: list = []
    neg_dest_pk: list = []
    invalid_pk: list = []
    neg_output: list = []
    not_enough_value: list = []
    invalid_coinbase: list = []

    def __init__(self, tx_df: pd.DataFrame, in_df: pd.DataFrame, out_df: pd.DataFrame):
        self.tx_df = tx_df
        self.in_df = in_df
        self.out_df = out_df

    def write_out(self):
        """Store validated dataset, related analytics and unspent outputs ids as of the last block.
        """

        # Store validated data-set
        tx_outf = VALIDATED_FOLDER + 'validated_transactions.csv'
        in_outf = VALIDATED_FOLDER + 'validated_inputs.csv'
        out_outf = VALIDATED_FOLDER + 'validated_outputs.csv'
        self.tx_df.to_csv(tx_outf, index=False)
        self.in_df.to_csv(in_outf, index=False)
        self.out_df.to_csv(out_outf, index=False)

        # Store analytics
        json_data = {
            'tot_invalid': self.n_invalid, 
            'not_in_utxo': {
                'tot': len(self.not_in_utxo), 
                'tx_ids': self.not_in_utxo
            },
            'neg_dest_pk': {
                'tot': len(self.neg_dest_pk),
                'tx_ids': self.neg_dest_pk
            },
            'invalid_pk': {
                'tot': len(self.invalid_pk), 
                'tx_ids': self.invalid_pk
            },
            'neg_output': {
                'tot': len(self.neg_output), 
                'tx_ids': self.neg_output
            },
            'not_enough_value': {
                'tot': len(self.not_enough_value), 
                'tx_ids': self.not_enough_value
            },
            'invalid_coinbase': {
                'tot': len(self.invalid_coinbase),
                'tx_ids': self.invalid_coinbase
            }
        }
        with open(VALIDATION_FILE, 'w') as outf:
            json.dump(json_data, outf, indent=4)

        # Store UTXO IDs
        with open(UTXO_FILE, 'w') as outf:
            for id in self.utxo:
                outf.write(f'{id}\n')

    def is_valid(self, tx_id: int, inputs: pd.DataFrame, outputs: pd.DataFrame) -> bool:
        """Check if a transaction is valid
        
            :inputs:
                - <tx_id>: id of the tx
                - <inputs>: tx corresponding inputs
                - <outputs>: tx corresponding outputs
            :outputs:
                - True, if valid
                - False, otherwise
        """

        for row in inputs.values:
            # Check if in UTXO
            if row[3] not in self.utxo:
                self.n_invalid += 1
                self.not_in_utxo.append(tx_id)
                return False

            # Check SIG_ID == PK_ID
            if row[2] != self.out_df[self.out_df['id'] == row[3]]['pk_id'].iloc[0]:
                self.n_invalid += 1
                self.invalid_pk.append(tx_id)
                return False

            # Check PK_ID (dest. address) valid
            dest_outputs = self.out_df[self.out_df['tx_id'] == row[1]]['pk_id']
            for pk in dest_outputs:
                if pk <= 0 and pk != -1: # neg PK and not non-standard script
                    self.n_invalid += 1
                    self.neg_dest_pk.append(tx_id)
                    return False

        # Compute total inputs value
        tot_input = 0
        spending_outputs_list = []
        for out_id in inputs['out_id']:
            tot_input += self.out_df.loc[self.out_df['id'] == out_id]['value'].iloc[0]
            spending_outputs_list.append(out_id)
        # Check if inputs use same output (edge case of double spending)
        if len(spending_outputs_list) != len(set(spending_outputs_list)):
            self.n_invalid += 1
            self.not_in_utxo.append(tx_id)
            return False

        # Compute total outputs value
        tot_output = 0
        for value in outputs['value']:
            if value < 0: # neg output value
                self.n_invalid += 1
                self.neg_output.append(tx_id)
                return False
            else:
                tot_output += value

        # Check inputs values < outputs values
        if tot_input < tot_output:
            self.n_invalid += 1
            self.not_enough_value.append(tx_id)
            return False

        return True

    def drop_transaction(self, tx_id: int):
        """Drop an invalid transaction from all three dataframes by its id.
        
            :inputs:
                - <tx_id>: the id of the tx to drop
        """
        tx_indexes = self.tx_df[self.tx_df['id'] == tx_id].index
        in_indexes = self.in_df[self.in_df['tx_id'] == tx_id].index
        out_indexes = self.out_df[self.out_df['tx_id'] == tx_id].index
        self.tx_df.drop(tx_indexes, inplace=True)
        self.in_df.drop(in_indexes, inplace=True)
        self.out_df.drop(out_indexes, inplace=True)

    def validate_data(self):
        """Validate the data-set and store results."""

        # Drop coinbase txs with invalid reward (< 50BTC)
        coinbase_tx = self.in_df[self.in_df['sig_id'] == 0]['tx_id']
        coinbase_outputs = self.out_df[self.out_df['tx_id'].isin(coinbase_tx)].groupby('tx_id')['value'].sum()

        for v in coinbase_outputs.iteritems():
            tx_id, value = v[0], v[1]
            if value < MINING_REWARD:
                self.n_invalid += 1
                self.invalid_coinbase.append(tx_id)
                self.drop_transaction(tx_id)

        # Validate remaining transactions
        for tx_id in self.tx_df['id']:
            inputs = self.in_df[self.in_df['tx_id'] == tx_id]
            outputs = self.out_df[self.out_df['tx_id'] == tx_id]

            if inputs.loc[inputs.index[0], 'sig_id'] == 0 and inputs.loc[inputs.index[0], 'out_id'] == -1: # coinbase tx
                    # Add new unspent outputs
                    for out_id in outputs['id']:
                        self.utxo.add(out_id)
            elif self.is_valid(tx_id, inputs, outputs): # transfer tx
                # Remove spent outputs, add new unspent ones
                for out_id in inputs['out_id']:
                    self.utxo.remove(out_id)
                for id in outputs['id']:
                    self.utxo.add(id)
            else: # remove indalid entries
                self.drop_transaction(tx_id)

        # Store results
        self.write_out()

