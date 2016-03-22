#!/usr/bin/python

import amulet
import os
import time
import yaml

from charmhelpers.contrib.openstack.amulet.deployment import (
    OpenStackAmuletDeployment
)

# This file needs de-linted.  The (mis)use of n-o q-a below causes all lint 
# to go undetected.  Remove that & fixme.
from charmhelpers.contrib.openstack.amulet.utils import (
    OpenStackAmuletUtils,
    DEBUG, # flake8: noqa
    ERROR
)

# Use DEBUG to turn on debug logging
u = OpenStackAmuletUtils(DEBUG)

# XXX Tests inspecting relation data from the perspective of the
# neutron-openvswitch are missing because amulet sentries aren't created for
# subordinates Bug#1421388


class NeutronOVSBasicDeployment(OpenStackAmuletDeployment):
    """Amulet tests on a basic neutron-openvswtich deployment."""

    def __init__(self, series, openstack=None, source=None, git=False,
                 stable=False):
        """Deploy the entire test environment."""
        super(NeutronOVSBasicDeployment, self).__init__(series, openstack,
                                                        source, stable)
        self.git = git
        self._add_services()
        self._add_relations()
        self._configure_services()
        self._deploy()

        u.log.info('Waiting on extended status checks...')
        exclude_services = ['mysql']
        self._auto_wait_for_status(exclude_services=exclude_services)

        self._initialize_tests()

    def _add_services(self):
        """Add services

           Add the services that we're testing, where neutron-openvswitch is local,
           and the rest of the service are from lp branches that are
           compatible with the local charm (e.g. stable or next).
           """
        # Services and relations which are present merely to satisfy required_interfaces
        # and workload status are not inspected.  Fix me.  Inspect those too.
        this_service = {'name': 'neutron-openvswitch'}
        other_services = [{'name': 'nova-compute'},
                          {'name': 'rabbitmq-server'},
                          {'name': 'keystone'},  # satisfy workload stat
                          {'name': 'mysql'},  # satisfy workload stat
                          {'name': 'glance'},  # satisfy workload stat
                          {'name': 'neutron-api'}]
        super(NeutronOVSBasicDeployment, self)._add_services(this_service,
                                                             other_services)

    def _add_relations(self):
        """Add all of the relations for the services."""
        relations = {
            'neutron-openvswitch:amqp': 'rabbitmq-server:amqp',  
            'neutron-openvswitch:neutron-plugin':
            'nova-compute:neutron-plugin',
            'neutron-openvswitch:neutron-plugin-api':
            'neutron-api:neutron-plugin-api',
            # Satisfy workload stat:
            'neutron-api:identity-service': 'keystone:identity-service',
            'neutron-api:shared-db': 'mysql:shared-db',
            'neutron-api:amqp': 'rabbitmq-server:amqp',
            'nova-compute:amqp': 'rabbitmq-server:amqp',
            'nova-compute:image-service': 'glance:image-service',
            'glance:identity-service': 'keystone:identity-service',
            'glance:shared-db': 'mysql:shared-db',
            'glance:amqp': 'rabbitmq-server:amqp',
            'keystone:shared-db': 'mysql:shared-db',
        }
        super(NeutronOVSBasicDeployment, self)._add_relations(relations)

    def _configure_services(self):
        """Configure all of the services."""
        neutron_ovs_config = {}
        if self.git:
            amulet_http_proxy = os.environ.get('AMULET_HTTP_PROXY')

            branch = 'stable/' + self._get_openstack_release_string()

            if self._get_openstack_release() >= self.trusty_kilo:
                openstack_origin_git = {
                    'repositories': [
                        {'name': 'requirements',
                         'repository': 'git://github.com/openstack/requirements',
                         'branch': branch},
                        {'name': 'neutron-fwaas',
                         'repository': 'git://github.com/openstack/neutron-fwaas',
                         'branch': branch},
                        {'name': 'neutron-lbaas',
                         'repository': 'git://github.com/openstack/neutron-lbaas',
                         'branch': branch},
                        {'name': 'neutron-vpnaas',
                         'repository': 'git://github.com/openstack/neutron-vpnaas',
                         'branch': branch},
                        {'name': 'neutron',
                         'repository': 'git://github.com/openstack/neutron',
                         'branch': branch},
                    ],
                    'directory': '/mnt/openstack-git',
                    'http_proxy': amulet_http_proxy,
                    'https_proxy': amulet_http_proxy,
                }
            else:
                reqs_repo = 'git://github.com/openstack/requirements'
                neutron_repo = 'git://github.com/openstack/neutron'
                if self._get_openstack_release() == self.trusty_icehouse:
                    reqs_repo = 'git://github.com/coreycb/requirements'
                    neutron_repo = 'git://github.com/coreycb/neutron'

                openstack_origin_git = {
                    'repositories': [
                        {'name': 'requirements',
                         'repository': reqs_repo,
                         'branch': branch},
                        {'name': 'neutron',
                         'repository': neutron_repo,
                         'branch': branch},
                    ],
                    'directory': '/mnt/openstack-git',
                    'http_proxy': amulet_http_proxy,
                    'https_proxy': amulet_http_proxy,
                }
            neutron_ovs_config['openstack-origin-git'] = yaml.dump(openstack_origin_git)
        configs = {'neutron-openvswitch': neutron_ovs_config}
        super(NeutronOVSBasicDeployment, self)._configure_services(configs)

    def _initialize_tests(self):
        """Perform final initialization before tests get run."""
        # Access the sentries for inspecting service units
        self.compute_sentry = self.d.sentry.unit['nova-compute/0']
        self.rabbitmq_sentry = self.d.sentry.unit['rabbitmq-server/0']
        self.neutron_api_sentry = self.d.sentry.unit['neutron-api/0']
        self.n_ovs_sentry = self.d.sentry.unit['neutron-openvswitch/0']

    def test_100_services(self):
        """Verify the expected services are running on the corresponding
           service units."""
        u.log.debug('Checking system services on units...')

        services = {
            self.compute_sentry: ['nova-compute',
                                  'neutron-plugin-openvswitch-agent'],
            self.rabbitmq_sentry: ['rabbitmq-server'],
            self.neutron_api_sentry: ['neutron-server'],
        }

        ret = u.validate_services_by_name(services)
        if ret:
            amulet.raise_status(amulet.FAIL, msg=ret)

        u.log.debug('OK')

    def test_rabbitmq_amqp_relation(self):
        """Verify data in rabbitmq-server/neutron-openvswitch amqp relation"""
        unit = self.rabbitmq_sentry
        relation = ['amqp', 'neutron-openvswitch:amqp']
        expected = {
            'private-address': u.valid_ip,
            'password': u.not_null,
            'hostname': u.valid_ip
        }

        ret = u.validate_relation_data(unit, relation, expected)
        if ret:
            message = u.relation_error('rabbitmq amqp', ret)
            amulet.raise_status(amulet.FAIL, msg=message)

    def test_nova_compute_relation(self):
        """Verify the nova-compute to neutron-openvswitch relation data"""
        unit = self.compute_sentry
        relation = ['neutron-plugin', 'neutron-openvswitch:neutron-plugin']
        expected = {
            'private-address': u.valid_ip,
        }

        ret = u.validate_relation_data(unit, relation, expected)
        if ret:
            message = u.relation_error('nova-compute neutron-plugin', ret)
            amulet.raise_status(amulet.FAIL, msg=message)

    def test_neutron_api_relation(self):
        """Verify the neutron-api to neutron-openvswitch relation data"""
        unit = self.neutron_api_sentry
        relation = ['neutron-plugin-api',
                    'neutron-openvswitch:neutron-plugin-api']
        expected = {
            'private-address': u.valid_ip,
        }

        ret = u.validate_relation_data(unit, relation, expected)
        if ret:
            message = u.relation_error('neutron-api neutron-plugin-api', ret)
            amulet.raise_status(amulet.FAIL, msg=message)

    def process_ret(self, ret=None, message=None):
        if ret:
            amulet.raise_status(amulet.FAIL, msg=message)

    def check_ml2_setting_propagation(self, service, charm_key,
                                      config_file_key, vpair,
                                      section):

        # Needs love - test actions not clear in log
        unit = self.compute_sentry
        conf = "/etc/neutron/plugins/ml2/ml2_conf.ini"
        for value in vpair:
            self.d.configure(service, {charm_key: value})
            time.sleep(60)
            ret = u.validate_config_data(unit, conf, section,
                                         {config_file_key: value})
            msg = "Propagation error, expected %s=%s" % (config_file_key,
                                                         value)
            self.process_ret(ret=ret, message=msg)

    def test_l2pop_propagation(self):
        """Verify that neutron-api l2pop setting propagates to neutron-ovs"""

        # Needs love - not idempotent
        self.check_ml2_setting_propagation('neutron-api',
                                           'l2-population',
                                           'l2_population',
                                           ['False', 'True'],
                                           'agent')

    def test_nettype_propagation(self):
        """Verify that neutron-api nettype setting propagates to neutron-ovs"""

        # Needs love - not idempotent
        self.check_ml2_setting_propagation('neutron-api',
                                           'overlay-network-type',
                                           'tunnel_types',
                                           ['vxlan', 'gre'],
                                           'agent')

    def test_secgroup_propagation_local_override(self):
        """Verify disable-security-groups overrides what neutron-api says"""

        # Needs love - not idempotent
        unit = self.compute_sentry
        conf = "/etc/neutron/plugins/ml2/ml2_conf.ini"
        self.d.configure('neutron-api', {'neutron-security-groups': 'True'})
        self.d.configure('neutron-openvswitch',
                         {'disable-security-groups': 'True'})
        time.sleep(30)
        ret = u.validate_config_data(unit, conf, 'securitygroup',
                                     {'enable_security_group': 'False'})
        msg = "Propagation error, expected %s=%s" % ('enable_security_group',
                                                     'False')
        self.process_ret(ret=ret, message=msg)
        self.d.configure('neutron-openvswitch',
                         {'disable-security-groups': 'False'})
        self.d.configure('neutron-api', {'neutron-security-groups': 'True'})
        time.sleep(30)
        ret = u.validate_config_data(unit, conf, 'securitygroup',
                                     {'enable_security_group': 'True'})

    def test_z_restart_on_config_change(self):
        """Verify that the specified services are restarted when the
        config is changed."""

        sentry = self.n_ovs_sentry
        juju_service = 'neutron-openvswitch'

        # Expected default and alternate values
        set_default = {'debug': 'False'}
        set_alternate = {'debug': 'True'}

        # Services which are expected to restart upon config change,
        # and corresponding config files affected by the change
        conf_file = '/etc/neutron/neutron.conf'
        services = {
            'neutron-openvswitch-agent': conf_file
        }

        # Make config change, check for svc restart, conf file mod time change
        u.log.debug('Making config change on {}...'.format(juju_service))
        mtime = u.get_sentry_time(sentry)
        self.d.configure(juju_service, set_alternate)

        sleep_time = 60
        for s, conf_file in services.iteritems():
            u.log.debug("Checking that service restarted: {}".format(s))
            if not u.validate_service_config_changed(sentry, mtime, s,
                                                     conf_file,
                                                     sleep_time=sleep_time):
                self.d.configure(juju_service, set_default)
                msg = "service {} didn't restart after config change".format(s)
                amulet.raise_status(amulet.FAIL, msg=msg)

        self.d.configure(juju_service, set_default)
        u.log.debug('OK')
