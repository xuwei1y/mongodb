%%%-------------------------------------------------------------------
%%% @author yinchong
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%% 对mongodb驱动 顶层api的扩展和补充 用来支持 安全写入操作和 从库读取等功能
%%% @end
%%% Created : 07. 八月 2018 11:13
%%%-------------------------------------------------------------------
-module(mongo_extend_api).
-author("yinchong").

-include("mongoc.hrl").
-include("mongo_protocol.hrl").

%% API
-export([insert/4 , update/6 , delete/4 , delete_one/3 , delete_one/4 , count/3 , count/4 , update/5]).

-export([find/4 , find/6 , find/5 ]  ) .

-export([find_one_readmode/5 , find_one_readmode/6 , find_readmode/5 , find_readmode/6 , find_readmode/7 ]).

-export_type([read_host_mode/0]).
-type read_host_mode() :: primary | secondary | primaryPreferred | secondaryPreferred | nearest.


-spec find(atom() | pid(), colldb(), selector(), projector()) ->
    {ok, cursor()} | [].
find(Topology, {Db , Collection}, Selector, Projector) ->
    find( Topology, {Db , Collection}, Selector, Projector , 0 ) .

-spec find(atom() | pid(), colldb(), selector(), projector(), integer()) ->
    {ok, cursor()} | [].
find( Topology , {Db , Collection} , Selector , Projector , Skip ) ->
    mongoc:transaction_query(Topology,
                             fun( #{pool := Worker} = Conf) ->
                                 mc_worker_api:set_database( Worker , Db ) ,
                                 Query = mongoc:find_query(Conf, Collection, Selector, Projector, Skip, 0),
                                 case mc_worker_api:find(Worker, Query) of
                                     { ok , Cursor } ->

%%                                         mc_cursor:set_connection( Cursor , Worker ) ,
                                         Doc = mc_cursor:rest( Cursor ) ,
                                         mc_cursor:close( Cursor ) ,
                                         Doc ;
                                     _->[]
                                 end
                             end, #{}) .

-spec find(atom() | pid(), colldb(), selector(), projector(), integer() , integer() ) ->
    {ok, cursor()} | [].
find( Topology , {Db , Collection} , Selector , Projector , Skip , Limit ) ->
    mongoc:transaction_query(Topology,
                             fun( #{pool := Worker} = Conf) ->
                                 mc_worker_api:set_database( Worker , Db ) ,
                                 Query = mongoc:find_query(Conf, Collection, Selector, Projector, Skip, 0),
                                 case mc_worker_api:find(Worker, Query) of
                                     { ok , Cursor } ->
%%                                         mc_cursor:set_connection( Cursor , Worker ) ,
                                         Doc = mc_cursor:take( Cursor , Limit ) ,
                                         mc_cursor:close( Cursor ) ,
                                         Doc ;
                                     _->[]
                                 end
                             end, #{}) .



-spec find_readmode(atom() | pid(), colldb(), selector(), projector() , read_host_mode() ) ->
    {ok, cursor()} | [].
find_readmode(Topology, {Db , Collection}, Selector, Projector , ReadMode) ->
    case find_options( Topology , {Db , Collection}, Selector , Projector , #{ rp_mode => ReadMode } )of
        { ok , Cursor } ->
            mongoc:transaction_query(Topology,
                                     fun( #{pool := Worker}) ->
                                         mc_worker_api:set_database( Worker , Db ) ,
                                         mc_cursor:set_connection( Cursor , Worker ) ,
                                         Doc = mc_cursor:rest( Cursor ) ,
                                         mc_cursor:close( Cursor ) ,
                                         Doc
                                     end, #{ rp_mode => ReadMode });
        _->
            []
    end .

-spec find_readmode(atom() | pid(), colldb(), selector(), projector(), integer() , read_host_mode() ) ->
    {ok, cursor()} | [].
find_readmode( Topology , {Db , Collection} , Selector , Projector , Skip , ReadMode) ->
    case find_options( Topology , {Db , Collection} , Selector , Projector , Skip , 0 , #{ rp_mode => ReadMode } ) of
        { ok , Cursor } ->
            mongoc:transaction_query(Topology,
                                     fun( #{pool := Worker} ) ->
                                         mc_worker_api:set_database( Worker , Db ) ,
                                         mc_cursor:set_connection( Cursor , Worker ) ,
                                         Doc = mc_cursor:rest( Cursor ) ,
                                         mc_cursor:close( Cursor ) ,
                                         Doc
                                     end, #{ rp_mode => ReadMode });
        _->
            []
    end .

-spec find_readmode(atom() | pid(), colldb(), selector(), projector(), integer() , integer() , read_host_mode() ) ->
    {ok, cursor()} | [].
find_readmode( Topology , {Db , Collection} , Selector , Projector , Skip , Limit , ReadMode ) ->
    case find_options( Topology , {Db , Collection} , Selector , Projector , Skip , 0 , #{ rp_mode => ReadMode } ) of
        { ok , Cursor } ->
            mongoc:transaction_query(Topology,
                                     fun( #{pool := Worker} ) ->
                                         mc_worker_api:set_database( Worker , Db ) ,
                                         mc_cursor:set_connection( Cursor , Worker ) ,
                                         Doc = mc_cursor:take( Cursor , Limit ) ,
                                         mc_cursor:close( Cursor ) ,
                                         Doc
                                     end, #{ rp_mode => ReadMode });
        _->
            []
    end .


-spec find_one_readmode(atom() | pid(), colldb(), selector(), projector() , read_host_mode() ) ->
    map() | undefined.
find_one_readmode( Topology, {Db , Collection}, Selector, Projector , ReadMode ) ->
    mongoc:transaction_query(Topology,
                             fun(Conf = #{pool := Worker}) ->
                                 mc_worker_api:set_database( Worker , Db ) ,
                                 Query = mongoc:find_one_query(Conf, Collection, Selector, Projector, 0 ),
                                 mc_worker_api:find_one(Worker, Query)
                             end, #{ rp_mode => ReadMode }).

-spec find_one_readmode(atom() | pid(), colldb(), selector(), projector() ,  integer() , read_host_mode() ) ->
    map() | undefined.
find_one_readmode( Topology, {Db , Collection}, Selector, Projector , Skip , ReadMode ) ->
    mongoc:transaction_query(Topology,
                             fun(Conf = #{pool := Worker}) ->
                                 mc_worker_api:set_database( Worker , Db ) ,
                                 Query = mongoc:find_one_query(Conf, Collection, Selector, Projector, Skip ),
                                 mc_worker_api:find_one(Worker, Query)
                             end, #{ rp_mode => ReadMode }).



-spec find_options(atom() | pid(), colldb(), selector(), projector() , map() ) ->
    {ok, cursor()} | [].
find_options(Topology,{Db, Collection}, Selector, Projector , Options) ->
    find_options(Topology, {Db, Collection}, Selector, Projector, 0, 0 , Options).

-spec find_options(atom() | pid(), colldb(), selector(), projector(), integer(), integer() , map()  ) ->
    {ok, cursor()} | [].
find_options(Topology, {Db , Collection}, Selector, Projector, Skip, Batchsize , Options) ->
    mongoc:transaction_query(Topology,
                             fun(Conf = #{pool := Worker}) ->
                                 mc_worker_api:set_database( Worker , Db ) ,
                                 Query = mongoc:find_query(Conf, Collection, Selector, Projector, Skip, Batchsize),
                                 mc_worker_api:find(Worker, Query)
                             end, Options).



-spec insert(atom() | pid(), colldb(), list() | map() | bson:document() , boolean()) ->
    {{boolean(), map()}, list()}.
insert(Topology, {Db , Collection}, Document , WC) ->
    mongoc:transaction(Topology,
                       fun(#{pool := Worker}) ->
                           mc_worker_api:set_database( Worker , Db ) ,
                           mc_worker_api:insert(Worker, Collection, Document , WC )
                       end,
                       #{}).


-spec update(atom() | pid(), colldb(), selector(), map(), map()) ->
    {boolean(), map()}.
update(Topology, {Db , Collection}, Selector, Doc, Opts) ->
    Upsert = maps:get(upsert, Opts, false),
    MultiUpdate = maps:get(multi, Opts, false),
    mongoc:transaction(Topology,
                       fun(#{pool := Worker}) ->
                           mc_worker_api:set_database( Worker , Db ) ,
                           mc_worker_api:update(Worker, Collection, Selector, Doc, Upsert, MultiUpdate)
                       end, Opts).

-spec update(atom() | pid(), colldb(), selector(), map(), map() , bson:document()) ->
    {boolean(), map()}.
update(Topology, {Db , Collection}, Selector, Doc, Opts ,WC) ->
    Upsert = maps:get(upsert, Opts, false),
    MultiUpdate = maps:get(multi, Opts, false),
    mongoc:transaction(Topology,
                       fun(#{pool := Worker}) ->
                           mc_worker_api:set_database( Worker , Db ) ,
                           mc_worker_api:update(Worker, Collection, Selector, Doc, Upsert, MultiUpdate , WC )
                       end, Opts).


-spec delete(atom() | pid(), colldb(), selector() , bson:document() ) ->
    {boolean(), map()}.
delete(Topology, {Db , Collection}, Selector ,WC) ->
    mongoc:transaction(Topology,
                       fun(#{pool := Worker}) ->
                           mc_worker_api:set_database( Worker , Db ) ,
                           mc_worker_api:delete_limit(Worker, Collection, Selector ,  0 , WC )
                       end,
                       #{}).

-spec delete_one(atom() | pid(), colldb(), selector() ) ->
    {boolean(), map()}.
delete_one(Topology, {Db , Collection}, Selector) ->
    mongoc:transaction(Topology,
                       fun(#{pool := Worker}) ->
                           mc_worker_api:set_database( Worker , Db ) ,
                           mc_worker_api:delete_limit(Worker, Collection, Selector ,1)
                       end,
                       #{}).

-spec delete_one(atom() | pid(), colldb(), selector() , bson:document() ) ->
    {boolean(), map()}.
delete_one(Topology, {Db , Collection}, Selector , WC) ->
    mongoc:transaction(Topology,
                       fun(#{pool := Worker}) ->
                           mc_worker_api:set_database( Worker , Db ) ,
                           mc_worker_api:delete_limit(Worker, Collection, Selector ,1 , WC )
                       end,
                       #{}).




-spec count(atom() | pid(), colldb(), selector() ) -> integer().
count(Topology, {Db , Collection}, Selector ) ->
    mongoc:transaction_query(Topology,
                             fun(Conf = #{pool := Worker}) ->
                                 mc_worker_api:set_database( Worker , Db ) ,
                                 Query = mongoc:count_query(Conf, Collection, Selector, 0),
                                 mc_worker_api:count(Worker, Query)
                             end,
                             #{}).


-spec count(atom() | pid(), colldb(), selector(), integer()) -> integer().
count(Topology,  {Db , Collection}, Selector, Limit) ->
    mongoc:transaction_query(Topology,
                             fun(Conf = #{pool := Worker}) ->
                                 mc_worker_api:set_database( Worker , Db ) ,
                                 Query = mongoc:count_query(Conf, Collection, Selector, Limit),
                                 mc_worker_api:count(Worker, Query)
                             end,
                             #{}).