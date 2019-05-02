from operator import attrgetter

from ryu.app import simple_switch_13
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub
from mldcs.client import mongodb

mdb = mongodb.MongoDBWrapper()


class SimpleMonitor13(simple_switch_13.SimpleSwitch13):

    def __init__(self, *args, **kwargs):
        super(SimpleMonitor13, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.monitor_thread = hub.spawn(self._monitor)

    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.info('register datapath: %016x', datapath.id)
                mdb.save_datapath(datapath.id, datapath.address)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.info('unregister datapath: %016x', datapath.id)
                mdb.remove_datapath(datapath.id)
                del self.datapaths[datapath.id]

    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(3)

    def _request_stats(self, datapath):
        self.logger.debug('send stats request: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        self.logger.info("Received StatsReply from datapath {}".format(ev.msg.datapath.id))

        for stat in sorted([flow for flow in body if flow.priority == 1],
                           key=lambda flow: (flow.match['in_port'],
                                             flow.match['eth_dst'])):
            # self.logger.error("Match {}".format(stat.match.to_jsondict()))
            # self.logger.error("Table ID: {}".format(stat.table_id))
            actions = []
            for action in stat.instructions:
                action = action.__dict__
                # self.logger.error("Actions: {}".format(action))
                a = []
                for i in action['actions']:
                    a.append(i.__dict__)
                action['actions'] = a
                actions.append(action)
            
            # check if flow exist
            data = {
                'datapath_id': ev.msg.datapath.id,
                'table_id': stat.table_id,
                'match': stat.match.to_jsondict(),
            }
            result = mdb.check_exist_flow(**data)
            malicious_flow = False
            # TODO check if the flow is malicious or not
            if not result and not malicious_flow:
                # Save new flow
                self.logger.info("Save a new flow into database")
                data['actions'] = actions
                data['priority'] = stat.priority
                mdb.save_flow(**data)

            if result:
                # self.logger.info("Before {}".format(result[0]['priority']))
                # self.logger.info("After Prioriry {} ".format(stat.priority))
                if len(result) > 0:
                    # checking here
                    if result[0]['priority'] != stat.priority and result[0]['actions'] == actions:
                        self.logger.info("The priority of the flow {} in datapath {} is changed".format(
                            result[0]['_id'], ev.msg.datapath.id
                        ))
                    if result[0]['actions'] != actions and result[0]['priority'] == stat.priority:
                        self.logger.info("The actions of the flow {} in datapath {} is changed".format(
                            result[0]['_id'], ev.msg.datapath.id
                        ))
                    if result[0]['actions'] != actions and result[0]['priority'] != stat.priority:
                        self.logger.info("The actions and priority of the flow {} in datapath {} is changed".format(
                            result[0]['_id'], ev.msg.datapath.id
                        ))
            # self.logger.info('%016x %8x %17s %8x %8d %8d',
            #                  ev.msg.datapath.id,
            #                  stat.match['in_port'], stat.match['eth_dst'],
            #                  stat.instructions[0].actions[0].port,
            #                  stat.packet_count, stat.byte_count)

    # @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    # def _port_stats_reply_handler(self, ev):
    #     body = ev.msg.body
    #     self.logger.info('datapath         port     '
    #                      'rx-pkts  rx-bytes rx-error '
    #                      'tx-pkts  tx-bytes tx-error')
    #     self.logger.info('---------------- -------- '
    #                      '-------- -------- -------- '
    #                      '-------- -------- --------')
    #     for stat in sorted(body, key=attrgetter('port_no')):
    #         self.logger.info('%016x %8x %8d %8d %8d %8d %8d %8d',
    #                          ev.msg.datapath.id, stat.port_no,
    #                          stat.rx_packets, stat.rx_bytes, stat.rx_errors,
    #                          stat.tx_packets, stat.tx_bytes, stat.tx_errors)
