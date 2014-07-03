module Database.Zookeeper.ServiceRegistry 
( withZkRegistry
, register
, get
, Server
, Service
, Protocol
, ZkRegistry
, ZkException(..)
) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Data.Either
import Data.List
import Data.Maybe
import Data.Typeable
import qualified Data.ByteString.Char8 as BS
import qualified Database.Zookeeper as Zk

type Server = String
type Service = String
type Protocol = String

data Entry = Entry Service Protocol Server

data ZkRegistry = ZkRegistry Zk.Zookeeper (MVar [Entry])

newtype ZkException = ZkException String deriving (Eq, Show, Typeable)
instance Exception ZkException

withZkRegistry :: Server -> (ZkRegistry -> IO a) -> IO a
withZkRegistry server f =  do
  waitState  <- newEmptyMVar
  entries <- newMVar []
  let watcher _ _ e _ = putMVar waitState e
  Zk.withZookeeper server 5000 (Just watcher) Nothing $ \zk -> do
    state <- takeMVar waitState
    let registry = ZkRegistry zk entries
    -- todo $ forkIO keepAlive
    case state of
      Zk.ConnectedState -> f registry
      state             -> throw $ ZkException $ 
                             "Zookeeper connection failed: " ++ show state

register :: ZkRegistry -> Service -> Protocol -> Server -> IO ()
register (ZkRegistry zk entries) service protocol server = do
  let entry = Entry service protocol server
  modifyMVar_ entries $ return . (entry:)
  registerInternal zk entry

get :: ZkRegistry -> Service -> Protocol -> IO [Server]
get (ZkRegistry zk _) service protocol = do
  let path = mkPath ["services", service, protocol]
  children <- Zk.getChildren zk path Nothing
  case children of
    Left  _    -> return []
    Right keys -> do
      statEs <- forM keys $ \key -> Zk.get zk (path ++ "/" ++ key) Nothing
      let stats = rights statEs
      return $ map BS.unpack $ catMaybes $ fst `map` stats


zkPerm :: Zk.Acl
zkPerm = Zk.Acl "world" "anyone" [Zk.CanRead, Zk.CanWrite, Zk.CanCreate, Zk.CanDelete, Zk.CanAdmin]

mkPath :: [String] -> String
mkPath parts = "/" ++ foldr1 (\part path -> part ++ "/" ++ path) parts

mkdirp :: Zk.Zookeeper -> [String] -> IO ()
mkdirp zk parts = forM_ (tail $ inits parts) $ \partialPath -> 
  Zk.create zk (mkPath partialPath) Nothing (Zk.List [zkPerm]) []

registerInternal :: Zk.Zookeeper -> Entry -> IO ()
registerInternal zk (Entry service protocol server) = do
  let parts = ["services", service, protocol]
  mkdirp zk parts
  Zk.create zk (mkPath parts ++ "/") (Just $ BS.pack server) (Zk.List [zkPerm]) [Zk.Sequence, Zk.Ephemeral]
  return ()
