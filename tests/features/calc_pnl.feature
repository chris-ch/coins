Feature: P&L Calculation
  Calculating P&L for a portfolio

  Scenario: Calculate P&L when there is no trade
    Given following inflows and outflows
         | date                 | amount   | currency  |
         | 2017-07-01 01:05:00  | 100      | ETH       |
         | 2017-07-01 05:30:00  | 50000    | ETH       |
         | 2017-07-15 08:10:00  | -50000   | ETH       |
    And the reference currency is ETH
    And as of date is 2017/07/31 00:00:00
    Then P&L should be
         | date                 | P&L  |
         | 2017-07-01 01:05:00  | 0    |

  Scenario: Calculate P&L with some trades
    Given following inflows and outflows
         | date                 | amount   | currency  |
         | 2017-07-01 01:05:00  | 100      | USD       |
         | 2017-07-01 05:30:00  | 50000    | USD       |
         | 2017-07-15 08:10:00  | -50000   | USD       |
    And the reference currency is USD
    And as of date is 2017/07/31 00:00:00
    And following trades are performed
         | date                 | amount   | traded currency  | using currency  | at price |
         | 2017-07-01 01:08:30  | 50       | EUR              | USD             | 1.10     |
         | 2017-07-01 02:15:00  | 20       | CHF              | JPY             | 115      |
         | 2017-07-02 01:24:00  | -20      | CHF              | JPY             | 110      |
         | 2017-07-03 06:08:00  | 50000    | CHF              | EUR             | 1.05     |
         | 2017-07-03 10:07:00  | -20000   | CHF              | EUR             | 1.08     |
    And forex rates are
         | date                 | currency   | rate for 1 USD  |
         | 2017-07-01 00:00:00  | EUR        | 0.90            |
         | 2017-07-01 12:00:00  | EUR        | 0.88            |
         | 2017-07-02 00:00:00  | EUR        | 0.85            |
         | 2017-07-02 12:00:00  | EUR        | 0.91            |
         | 2017-07-03 00:00:00  | EUR        | 0.94            |
         | 2017-07-03 12:00:00  | EUR        | 0.92            |
         | 2017-07-01 00:00:00  | CHF        | 0.85            |
         | 2017-07-01 12:00:00  | CHF        | 0.86            |
         | 2017-07-02 00:00:00  | CHF        | 0.84            |
         | 2017-07-02 12:00:00  | CHF        | 0.85            |
         | 2017-07-03 00:00:00  | CHF        | 0.87            |
         | 2017-07-03 12:00:00  | CHF        | 0.88            |
         | 2017-07-01 00:00:00  | JPY        | 112             |
         | 2017-07-01 12:00:00  | JPY        | 110             |
         | 2017-07-02 00:00:00  | JPY        | 113             |
         | 2017-07-02 12:00:00  | JPY        | 111             |
         | 2017-07-03 00:00:00  | JPY        | 110             |
         | 2017-07-03 12:00:00  | JPY        | 109             |
    Then P&L should be
         | date                 | P&L  |
         | 2017-07-01 01:08:30  | 0    |
         | 2017-07-01 02:15:00  | 0    |
         | 2017-07-02 01:24:00  | 0    |
         | 2017-07-03 06:08:00  | 0    |
         | 2017-07-03 10:07:00  | 0    |
