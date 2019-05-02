#!/usr/bin/python
# -*- coding: utf-8 -*-
from pox.core import core
from pox import log
from pox.log import color

from pox.lib.util import dpid_to_str
from pox.lib.recoco import Timer
import pox.openflow.libopenflow_01 as of

from mldcs.client import mongodb
from pprint import pprint

LOGGER = core.getLogger()

mdb = mongodb.MongoDBWrapper()


class StatsHandle(object):

    def __init__(self):
        core.openflow.addListeners(self)
        self.switchs = {}

    def translate_flow(self, flow):
        data = {  # "length": len(flow),
                    # "duration_sec": flow.duration_sec,
                    # "duration_nsec": flow.duration_nsec,
                    # "idle_timeout": flow.idle_timeout,
                    # "hard_timeout": flow.hard_timeout,
                    # "cookie": flow.cookie,
            'table_id': flow.table_id,
            'priority': flow.priority,
            # 'packet_count': flow.packet_count,
            # 'byte_count': flow.byte_count,
            'actions': [action.__dict__ for action in flow.actions]
            }
        match = self.create_match_dict(flow.match.__dict__)
        data['match'] = match
        return data

    def create_match_dict(self, match):
        for (k, v) in match.iteritems():
            if type(v) not in [str, int]:
                match[k] = str(v)
        return match

    def _handle_ConnectionUp(self, event):
        switch_id = event.dpid
        switch_features = event.ofp
        connection = event.connection
        sock = connection.sock
        (ip, port) = sock.getpeername()
        host_ports = []
        all_ports = []
        if switch_id not in self.switchs:
            self.switchs[switch_id] = ip
            LOGGER.info('Add: switch=%s -> ip=%s',
                        dpid_to_str(switch_id), ip)

        mdb.save_datapath(dpid_to_str(switch_id), ip)
        LOGGER.info('List switch {}'.format(self.switchs))

    def tracking_flow(self, datapath_id, stats):
        datapath_id = dpid_to_str(datapath_id)
        print("Received Stats reply from {}".format(datapath_id))
        for flow in stats:
            data = self.translate_flow(flow)
            LOGGER.debug(data)
            # if not exist
            flow_db = mdb.check_exist_flow(
                    datapath_id=datapath_id,
                    table_id=data.get('table_id'),
                    match=data.get('match'))
            if flow_db:
                flow_db = flow_db[0]
                LOGGER.info("Flow is exist")
                # The flow is exist
                if flow_db.get('priority') != data.get('priority') and flow_db.get('match') == data.get('match'):
                    LOGGER.info("The flow is changed priority. Old priority {}, New priority {}".format(
                        flow_db.get('priority'), data.get('priority')
                    ))
                if flow_db.get('priority') == data.get('priority') and flow_db.get('actions') != data.get('actions'):
                    LOGGER.info("The flow is changed actions. Old actions {}, New actions {}".format(
                        flow_db.get('actions'), data.get('actions')
                    ))
                if flow_db.get('priority') != data.get('priority') and flow_db.get('actions') != data.get('actions'):
                    LOGGER.info("The flow is changed actions. Old actions {}, New actions {}".format(
                        flow_db.get('actions'), data.get('actions')
                    ))
            else:
                # Check if flow is malicious

                # check if it is malicious flow
                # is_malicious_flow = False
                # if not check
                data['datapath_id'] = datapath_id
                mdb.save_flow(data)
                LOGGER.info("Add new flow to database")

    def _handle_FlowStatsReceived(self, event):
        flows = self.tracking_flow(event.dpid, event.stats)

def _request_stats():
    for connection in core.openflow._connections.values():
        connection.send(of.ofp_stats_request(body=of.ofp_flow_stats_request()))
        LOGGER.info('Sent {} flow/port stats requests'.format(connection))


def launch():
    color.launch()
    core.registerNew(StatsHandle)
    LOGGER.info('Start pox application')
    Timer(3, _request_stats, recurring=True)
    LOGGER.info('Started period collect metric')