import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Analytics filenames
UTXO_FILE = r'bitcoin-analysis/data/analytics/utxo_ids.txt'
UTXO_ANALYTICS_FILE = r'bitcoin-analysis/data/analytics/utxo_analytics.json'

# Constants
BTC_SATOSHI_RATIO = 100_000_000


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        """Makes numpy/pandas integers JSON serializable."""
        if isinstance(obj, np.integer):
            return int(obj)
        return super(NpEncoder, self).default(obj)


class DataAnalyzer():

    # Dataframes
    tx_df: pd.DataFrame
    in_df: pd.DataFrame
    out_df: pd.DataFrame

    def __init__(self, tx_df: pd.DataFrame, in_df: pd.DataFrame, out_df: pd.DataFrame):
        self.tx_df = tx_df
        self.in_df = in_df
        self.out_df = out_df

    def write_txt(self, filename: str, data: dict):
        """Store analytics in a .txt file."""
        with open(filename, 'w') as outf:
            for k in data:
                outf.write(f'{k}: {data[k]}\n')

    def write_json(self, filename: str, json_data: dict):
        """Store analytics in a .json file."""
        with open(filename, 'w') as outf:
            json.dump(json_data, outf, indent=4, cls=NpEncoder)

    def plot_chart(self, data, filename: str, xlabel: str, ylabel: str, marker=None, color='#2986cc'):
        """Plot and save a default chart."""

        data.plot(
            legend=False, 
            xlabel=xlabel, 
            ylabel=ylabel, 
            marker=marker, 
            color=color,
        )
        plt.gca().spines['top'].set_visible(False)
        plt.gca().spines['right'].set_visible(False)
        plt.grid(True, linestyle='--')
        plt.savefig(filename)
        plt.clf()

    def plot_scatter(self, xdata, ydata, filename: str, xlabel: str, ylabel: str, color='#2986cc'):
        """Plot and save a scatter (distribution) chart."""

        plt.scatter(xdata, ydata, alpha=0.5, color=color)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.gca().spines['top'].set_visible(False)
        plt.gca().spines['right'].set_visible(False)
        plt.grid(True, linestyle='--')
        plt.savefig(filename)
        plt.clf()

    def get_utxo(self):
        """Compute and store UTXO analytics."""

        utxo = set()
        utxo_tot = 0

        # Load UTXO IDs at last block from validation phase
        with open(UTXO_FILE) as inf:
            lines = inf.readlines()
            # Compute total UTXO
            for line in lines:
                utxo.add(int(line))
                utxo_tot += self.out_df.loc[self.out_df['id'] == int(line)]['value'].iloc[0]
        
        # Compute maximum UTXO 
        idx = self.out_df[self.out_df['id'].isin(utxo)]['value'].idxmax()
        row = self.out_df.iloc[[idx]]

        out_id, tx_id, pk_id, value = row[['id', 'tx_id', 'pk_id', 'value']].iloc[0]
        block_id = self.tx_df[self.tx_df['id'] == tx_id]['block_id'].iloc[0]
        json_data = {
            'n_utxo': len(utxo),
            'tot_utxo_value': utxo_tot/BTC_SATOSHI_RATIO,
            'max_utxo': {
                'tx_id': tx_id,
                'block_id': block_id,
                'output_index': out_id,
                'output_address': pk_id,
                'value': value/BTC_SATOSHI_RATIO,
            }
        } 
        self.write_json(UTXO_ANALYTICS_FILE, json_data)

    def block_occupancy_analytics(self):
        """Compute and plot block occupancy analytics."""

        # Block occupancy for each block
        blocks_occupancy = self.tx_df.groupby('block_id').count()
        self.plot_chart(
            data=blocks_occupancy,
            filename='bitcoin-analysis/data/plots/block_occupancy.jpg',
            xlabel='Block IDs',
            ylabel='Block occupancy (# of transactions)',
        )

        # Block occupancy distribution
        bo_distribution = {}
        for v in blocks_occupancy.itertuples():
            value = v[1]
            if value in bo_distribution:
                bo_distribution[value] += 1
            else:
                bo_distribution[value] = 1

        self.plot_scatter(
            xdata=bo_distribution.keys(),
            ydata=bo_distribution.values(),
            filename='bitcoin-analysis/data/plots/block_occupancy_distribution.jpg',
            xlabel='Block occupancy (# of transactions)',
            ylabel='Frequency',
        )

        # Block occupancy evolution w.r.t. months
        month_blocks = ((365 * 24 * 60) / 12) / 10 # 1 block -> 10 minutes
        blocks_month = blocks_occupancy.groupby(blocks_occupancy.index // month_blocks).mean() # average occupancy per month
        self.plot_chart(
            data=blocks_month,
            filename='bitcoin-analysis/data/plots/block_size_monthly.jpg',
            xlabel='Months',
            ylabel='# of transactions (average)',
            marker='.',
        )

    def received_btc_analytics(self):
        """Compute and plot received bitcoin analytics."""

        # Get PKs with at least one coinbase
        coinbase_tx = self.in_df[self.in_df['sig_id'] == 0]['tx_id']
        pks = self.out_df[self.out_df['tx_id'].isin(coinbase_tx)]['pk_id']

        # Received bitcoins for each public key
        pks_groups = self.out_df[self.out_df['pk_id'].isin(pks)].groupby('pk_id')
        btc_received = pks_groups['value'].sum() / BTC_SATOSHI_RATIO # Satoshi to BTC
        self.plot_chart(
            data=btc_received,
            filename='bitcoin-analysis/data/plots/received_bitcoin.jpg',
            xlabel='Public keys IDs',
            ylabel='# of BTC',
            color='#f2a900',
        )

        # Received bitcoins distribution
        btc_distribution = {}
        for v in btc_received.iteritems():
            value = v[1]
            if value in btc_distribution:
                btc_distribution[value] += 1
            else:
                btc_distribution[value] = 1

        self.plot_scatter(
            xdata=btc_distribution.keys(),
            ydata=btc_distribution.values(),
            filename='bitcoin-analysis/data/plots/received_bitcoin_distribution.jpg',
            xlabel='BTC received',
            ylabel='Frequency',
            color='#f2a900',
        )

    def fees_analytics(self):
        """Compute and plot fees analytics."""

        # Drop coinbase tx inputs
        coinbase_tx = self.in_df[self.in_df['sig_id'] == 0]['tx_id']
        indexes = self.in_df[self.in_df['tx_id'].isin(coinbase_tx)].index
        self.in_df.drop(indexes, inplace=True)

        # Merge inputs-outputs based on <out_id>
        merge_df = pd.merge(self.in_df, self.out_df[['id', 'value']], left_on='out_id', right_on='id')

        # Drop coinbase tx outputs
        indexes = self.out_df[self.out_df['tx_id'].isin(coinbase_tx)].index
        self.out_df.drop(indexes, inplace=True)

        # Group inputs and outputs by <tx_i>
        txs_inputs = merge_df.groupby('tx_id')
        txs_outputs = self.out_df.groupby('tx_id')

        # Fees for each transaction
        txs_fees = (txs_inputs['value'].sum() - txs_outputs['value'].sum()) /BTC_SATOSHI_RATIO # Satoshi to BTC
        self.plot_chart(
            data=txs_fees,
            filename='bitcoin-analysis/data/plots/fees.jpg',
            xlabel='Transactions IDs',
            ylabel='Fees (BTC)',
            color='r',
        )

        # Fees distribution
        fees_distribution = {}
        for v in txs_fees.iteritems():
            fee = v[1]
            if fee in fees_distribution:
                fees_distribution[fee] += 1
            else:
                fees_distribution[fee] = 1

        self.plot_scatter(
            xdata=fees_distribution.keys(),
            ydata=fees_distribution.values(),
            filename='bitcoin-analysis/data/plots/fees_distribution.jpg',
            xlabel='Fees (BTC)',
            ylabel='Frequency',
            color='r',
        )

        # Compute fees distribution mapped to total input values
        txs_values = txs_inputs['value'].sum() / BTC_SATOSHI_RATIO

        values, fees = [], []
        for value, fee in zip(txs_values.iteritems(), txs_fees.iteritems()):
            values.append(value[1])
            fees.append(fee[1])
        self.plot_scatter(
            xdata=values,
            ydata=fees,
            filename='bitcoin-analysis/data/plots/value_fees_distribution.jpg',
            xlabel='Total input (BTC)',
            ylabel='Fees (BTC)',
            color='k',
        )

        # Compute fees distribution mapped to number of inputs
        inputs_count = self.in_df.groupby(['tx_id'])['tx_id'].count()

        n_inputs, fees = [], []
        for cont, fee in zip(inputs_count.iteritems(), txs_fees.iteritems()):
            n_inputs.append(cont[1])
            fees.append(fee[1])
        self.plot_scatter(
            xdata=n_inputs,
            ydata=fees,
            filename='bitcoin-analysis/data/plots/inputs_fees_distribution.jpg',
            xlabel='# of inputs',
            ylabel='Fees (BTC)',
            color='k',
        )