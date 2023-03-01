# dfk-event-logger
## _Capture events from DFK Contracts_

_dfk-event-logger_ saves events from DFK contracts across its various chains.

## Usage

    $ npm i # install required packages

Edit `src/dfk-event-logger.ts`:
The `config` object should be updated as necessary.
Valid values for `chain` are `hmy`, `dfk`, and `kla`.
Valid values for `contractName` are `hero_auction`, `meditation`, and `quest`.


Compile `src/dfk-event-logger.ts` - I use the build task in vscode which outputs to `dist/dfk-event-logger.js`.

Start the logger from your terminal:

    $ cd dist
    $ node dfk-event-logger.js

Make sure your terminal has enough lines to show the full status (25 is recommended).

### Adding more contracts
Update the `contractMap` object with a new top level key that is the contract name. Fill in the addresses and blocks that the contracts were deployed. List the events to capture in the `events` array.
