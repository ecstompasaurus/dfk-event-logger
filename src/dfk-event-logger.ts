import fs from 'fs';
import { ethers } from 'ethers';
import { z } from 'zod';
import t from 'terminal-kit';

const config: Config = {
  logDirectory: '../event_logs',
  abiDirectory: '../contract_abis',
  logSizeLimitMB: 10,
  contractName: 'meditation',
  chain: 'hmy',
};

// CHAIN AND CONTRACT CONFIGURATION

const chainInfo = {
  kla: {
    rpcURL: 'https://klaytn-mainnet-rpc.allthatnode.com:8551',
    desiredEventCount: 800,
    querySizeStep: 100,
    querySizeMin: 100,
    querySizeMax: 9000,
  },
  dfk: {
    rpcURL:
      'https://avax-dfk.gateway.pokt.network/v1/lb/6244818c00b9f0003ad1b619/ext/bc/q2aTwKuyzgs8pynF7UXBZCU7DejbZbZ6EUyHr3JQzYgwNPUPi/rpc',
    // 'https://subnets.avax.network/defi-kingdoms/dfk-chain/rpc',
    desiredEventCount: 800,
    querySizeStep: 100,
    querySizeMin: 100,
    querySizeMax: 5000,
  },
  hmy: {
    rpcURL: 'https://harmony-0-rpc.gateway.pokt.network',
    desiredEventCount: 800,
    querySizeStep: 50,
    querySizeMin: 100,
    querySizeMax: 950,
  },
};

const contractMap = {
  hero_auction: {
    events: ['AuctionCreated', 'AuctionSuccessful', 'AuctionCancelled'],
    kla: {
      address: '0x7F2B66DB2D02f642a9eb8d13Bc998d441DDe17A8',
      deployBlock: 108438187,
    },
    dfk: {
      address: '0xc390fAA4C7f66E4D62E59C231D5beD32Ff77BEf0',
      deployBlock: 1506994,
    },
    hmy: {
      address: '0x13a65B9F8039E2c032Bc022171Dc05B30c3f2892',
      deployBlock: 18597670,
    },
  },
  meditation: {
    events: [
      'MeditationBegun',
      'MeditationBegunWithLevel',
      'MeditationCompleted',
      'LevelUp',
      'StatUp',
    ],
    kla: {
      address: '0xdbEE8C336B06f2d30a6d2bB3817a3Ae0E34f4900',
      deployBlock: 108392439,
    },
    dfk: {
      address: '0xD507b6b299d9FC835a0Df92f718920D13fA49B47',
      deployBlock: 4544730,
    },
    hmy: {
      address: '0x0594D86b2923076a2316EaEA4E1Ca286dAA142C1',
      deployBlock: 20259186,
    },
  },
  quest: {
    events: ['QuestCompleted', 'QuestStarted'],
    kla: { address: '0x8dc58d6327E1f65b18B82EDFb01A361f3AAEf624', deployBlock: 108396457 },
    dfk: { address: '0xE9AbfBC143d7cef74b5b793ec5907fa62ca53154', deployBlock: 4249158 },
    hmy: { address: '0xAa9a289ce0565E4D6548e63a441e7C084E6B52F6', deployBlock: 25523674 },
  },
};

// TYPES

const chainSchema = z.enum(['hmy', 'dfk', 'kla']);
type Chain = z.infer<typeof chainSchema>;

const contractNameSchema = z.enum(['hero_auction', 'meditation', 'quest']);
type ContractName = z.infer<typeof contractNameSchema>;

const configSchema = z.object({
  logDirectory: z.string(),
  abiDirectory: z.string(),
  logSizeLimitMB: z.number(),
  contractName: contractNameSchema,
  chain: chainSchema,
});
type Config = z.infer<typeof configSchema>;

const streamSchema = z.object({
  logIndex: z.number(),
  logSize: z.number(),
  linesWritten: z.number(),
  stream: z.instanceof(fs.WriteStream),
});
type Stream = z.infer<typeof streamSchema>;

// INITIALIZE TERMINAL

t.terminal.fullscreen(true);
t.terminal.moveTo(1, 1);

// SET UP INITIAL CONTEXT

type ContextType = {
  shouldExit: boolean;
  writingLogs: boolean;
  terminal: t.Terminal;
  progressBar: t.Terminal.ProgressBarController;
  streams: { [index: string]: Stream };
  interface: ethers.utils.Interface;
  eventTopics: { [key: string]: string };
  totalBarItems: number;
  completeBarItems: number;
  provider: ethers.providers.Provider;
  endingBlock: number;
  nextBlockToProcess: number;
  querySize: number;
  blocksProcessed: number;
};

const abiFileName = `${config.abiDirectory}/${config.contractName}.json`;
const abi = JSON.parse(String(fs.readFileSync(abiFileName)));
const progressBar = t.terminal.progressBar({
  eta: true,
  percent: true,
});

const context: ContextType = {
  shouldExit: false,
  writingLogs: false,
  terminal: t.terminal,
  progressBar: progressBar,
  streams: {},
  interface: new ethers.utils.Interface(abi),
  eventTopics: {},
  totalBarItems: 0,
  completeBarItems: 0,
  provider: new ethers.providers.JsonRpcProvider(chainInfo[config.chain].rpcURL),
  endingBlock: 0,
  nextBlockToProcess: 0,
  querySize: 0,
  blocksProcessed: 0,
};

const eventNames = Object.values(context.interface.events).map((e) => e.name);
for (const eventName of eventNames) {
  if (contractMap[config.contractName].events.includes(eventName)) {
    const eventKey = String(context.interface.getEventTopic(eventName));
    context.eventTopics[eventName] = eventKey;
    fs.mkdirSync(`${config.logDirectory}/${config.contractName}/${eventName}`, { recursive: true });
  }
}
context.endingBlock = Number(await callBlockchain(context.provider.getBlockNumber()));

// FINISH INITIALIZING TERMINAL

t.terminal.moveTo(1, 3);
t.terminal.white(`Contract     : ${config.contractName}`).nextLine(1);
t.terminal.white(`Chain        : ${config.chain}`).nextLine(1);
t.terminal.white(`Target Block : ${context.endingBlock}`).nextLine(1);

// FUNCTIONS

function getLogInfo(contractName: ContractName, chain: Chain) {
  const infoFileName = `${config.logDirectory}/${contractName}.json`;

  context.nextBlockToProcess = contractMap[contractName][chain].deployBlock;
  context.querySize = 100;
  if (fs.existsSync(infoFileName)) {
    const logInfo = JSON.parse(String(fs.readFileSync(infoFileName)));
    if (chain in logInfo) {
      context.nextBlockToProcess = logInfo[chain].nextBlockToProcess;
      context.querySize = logInfo[chain].querySize;
    }
  }
}

function writeLogInfo(contractName: ContractName, chain: Chain) {
  const info = {
    nextBlockToProcess: context.nextBlockToProcess,
    querySize: context.querySize,
  };

  const infoFileName = `${config.logDirectory}/${contractName}.json`;
  let logInfo = { [chain]: info };
  if (fs.existsSync(infoFileName)) {
    logInfo = JSON.parse(String(fs.readFileSync(infoFileName)));
    logInfo[chain] = info;
  }
  fs.writeFileSync(infoFileName, JSON.stringify(logInfo));
}

function parseLog(log: ethers.providers.Log) {
  const logData = context.interface.parseLog(log);

  const pieces: any[] = [log.blockNumber, log.transactionHash, log.transactionIndex];

  for (const event of Object.values(context.interface.events)) {
    if (event.name === logData.name) {
      pieces.push(...event.inputs.map((i) => logData.args[i.name]));
    }
  }
  return { name: logData.name, logEntry: `${pieces.join(',')}\n` };
}

function newQuerySize(querySize: number, logLength: number) {
  if (logLength < chainInfo[config.chain].desiredEventCount * 0.9) {
    return Math.min(
      querySize + chainInfo[config.chain].querySizeStep,
      chainInfo[config.chain].querySizeMax
    );
  }

  if (logLength > chainInfo[config.chain].desiredEventCount * 1.1) {
    return Math.max(
      querySize - chainInfo[config.chain].querySizeStep,
      chainInfo[config.chain].querySizeMin
    );
  }
  return querySize;
}

async function callBlockchain(funcToCall: Promise<any>): Promise<any> {
  let response = -1;
  let numFailures = 0;
  while (response === -1) {
    try {
      response = await funcToCall;
    } catch (err) {
      numFailures++;
      const sleepTime = Math.min(numFailures * 1000, 10000);
      context.terminal.moveTo(1, 10).eraseLine().blue(err);
      await new Promise((r) => setTimeout(r, sleepTime));
    }
  }
  return response;
}

function writeToLogStream(
  contractName: ContractName,
  eventName: string,
  chain: Chain,
  logEntry: string
) {
  const eventLogPrefix = `${config.logDirectory}/${contractName}/${eventName}/${contractName}-${eventName}-${chain}`;
  if (!Object.keys(context.streams).includes(eventName)) {
    fs.mkdirSync(`${config.logDirectory}/${contractName}/${eventName}`, { recursive: true });
    let logIndex = 0;
    let logFileName = `${eventLogPrefix}-${logIndex}.log`;
    let nextFileName = `${eventLogPrefix}-${logIndex + 1}.log`;
    while (fs.existsSync(nextFileName)) {
      logIndex++;
      logFileName = nextFileName;
      nextFileName = `${eventLogPrefix}-${logIndex + 1}.log`;
    }

    let logSize = 0;
    try {
      logSize = fs.statSync(logFileName).size;
    } catch (err) {}

    context.streams[eventName] = {
      logIndex,
      logSize,
      linesWritten: 0,
      stream: fs.createWriteStream(logFileName, { flags: 'a' }),
    };
  }

  context.streams[eventName].stream.write(logEntry);
  context.streams[eventName].logSize += logEntry.length;
  context.streams[eventName].linesWritten++;

  if (context.streams[eventName].logSize > config.logSizeLimitMB * 1024 * 1024) {
    context.streams[eventName].stream.end();

    const newLogFileName = `${eventLogPrefix}-${context.streams[eventName].logIndex}.log`;
    context.streams[eventName].stream = fs.createWriteStream(newLogFileName, { flags: 'w' });
    context.streams[eventName].logIndex++;
    context.streams[eventName].logSize = 0;
  }
}

function closeStreamsAndExit(code: number) {
  for (const stream of Object.values(context.streams)) {
    stream.stream.end();
  }
  // need to wait
  context.progressBar.stop();
  context.terminal.down(80);
  context.terminal.processExit(code);
}

async function updateTerminal() {
  let nextBlock = await callBlockchain(context.provider.getBlock(context.nextBlockToProcess));
  context.progressBar.update({ progress: context.completeBarItems / context.totalBarItems });
  context.terminal.moveTo(1, 5).eraseDisplayBelow().nextLine(1);
  let nextBlockTime = 'unknown';
  let nextBlockSec = 0;
  if (nextBlock) {
    nextBlockSec = nextBlock.timestamp;
    nextBlockTime = new Date(nextBlockSec * 1000).toLocaleString();
  }
  context.terminal
    .white('Next Block   : ')
    .cyan(`${context.nextBlockToProcess} (${nextBlockTime})`)
    .nextLine(1);
  context.terminal
    .white('Blocks Left  : ')
    .cyan(`${context.endingBlock - context.nextBlockToProcess}`)
    .nextLine(1);
  context.terminal.white('Query Size   : ').cyan(`${context.querySize}`).nextLine(1);
  context.terminal
    .nextLine(2)
    .white(`Events Collected in ${context.blocksProcessed} blocks :`)
    .nextLine(1);
  for (const eventName of Object.keys(context.eventTopics)) {
    context.terminal
      .white(`${eventName} : `)
      .cyan(eventName in context.streams ? context.streams[eventName].linesWritten : 0)
      .nextLine(1);
  }
}

// SIGNAL HANDLERS

const SIGINTCallback = () => {
  if (context.shouldExit || !context.writingLogs) {
    closeStreamsAndExit(0);
  }
  context.progressBar.update({
    title: 'Exiting',
    progress: 1,
  });
  context.shouldExit = true;
};
process.on('SIGINT', SIGINTCallback);

// MAIN

async function main() {
  getLogInfo(config.contractName, config.chain);

  if (context.endingBlock < context.nextBlockToProcess) closeStreamsAndExit(0);

  context.totalBarItems = context.endingBlock - context.nextBlockToProcess + 1;

  do {
    await updateTerminal();
    const toBlock = Math.min(
      context.nextBlockToProcess + context.querySize - 1,
      context.endingBlock
    );
    let logs = await callBlockchain(
      context.provider.getLogs({
        fromBlock: context.nextBlockToProcess,
        toBlock,
        address: contractMap[config.contractName][config.chain].address,
        topics: [Object.values(context.eventTopics)],
      })
    );

    context.writingLogs = true;
    for (const log of logs) {
      const nextstr = parseLog(log);
      writeToLogStream(config.contractName, nextstr.name, config.chain, nextstr.logEntry);
    }

    context.completeBarItems += toBlock - context.nextBlockToProcess;
    context.blocksProcessed += context.querySize;
    context.nextBlockToProcess = toBlock + 1;
    context.querySize = newQuerySize(context.querySize, logs.length);

    writeLogInfo(config.contractName, config.chain);
    context.writingLogs = false;
  } while (context.nextBlockToProcess < context.endingBlock && !context.shouldExit);
  closeStreamsAndExit(0);
}

main().catch((error) => {
  console.error(error);
  closeStreamsAndExit(1);
});
