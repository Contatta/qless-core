'''Test the queue functionality'''

from common import TestQless


class TestJobs(TestQless):
    '''We should be able to list jobs in various states for a given queue'''
    def test_malformed(self):
        '''Enumerate all the ways that the input can be malformed'''
        self.assertMalformed(self.lua, [
            ('jobs', 0, 'complete', 'foo'),
            ('jobs', 0, 'complete', 0, 'foo'),
            ('jobs', 0, 'running'),
            ('jobs', 0, 'running', 'queue', 'foo'),
            ('jobs', 0, 'running', 'queue', 0, 'foo'),
            ('jobs', 0, 'stalled'),
            ('jobs', 0, 'stalled`', 'queue', 'foo'),
            ('jobs', 0, 'stalled', 'queue', 0, 'foo'),
            ('jobs', 0, 'scheduled'),
            ('jobs', 0, 'scheduled', 'queue', 'foo'),
            ('jobs', 0, 'scheduled', 'queue', 0, 'foo'),
            ('jobs', 0, 'depends'),
            ('jobs', 0, 'depends', 'queue', 'foo'),
            ('jobs', 0, 'depends', 'queue', 0, 'foo'),
            ('jobs', 0, 'recurring'),
            ('jobs', 0, 'recurring', 'queue', 'foo'),
            ('jobs', 0, 'recurring', 'queue', 0, 'foo'),
            ('jobs', 0, 'foo', 'queue', 0, 25)
        ])

    def test_complete(self):
        '''Verify we can list complete jobs'''
        jids = map(str, range(10))
        for jid in jids:
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('pop', jid, 'queue', 'worker', 10)
            self.lua('complete', jid, jid, 'worker', 'queue', {})
            complete = self.lua('jobs', jid, 'complete')
            self.assertEqual(len(complete), int(jid) + 1)
            self.assertEqual(complete[0], jid)

    def test_running(self):
        '''Verify that we can get a list of running jobs in a queue'''
        jids = map(str, range(10))
        for jid in jids:
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('pop', jid, 'queue', 'worker', 10)
            running = self.lua('jobs', jid, 'running', 'queue')
            self.assertEqual(len(running), int(jid) + 1)
            self.assertEqual(running[-1], jid)

    def test_stalled(self):
        '''Verify that we can get a list of stalled jobs in a queue'''
        self.lua('config.set', 0, 'heartbeat', 10)
        jids = map(str, range(10))
        for jid in jids:
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('pop', jid, 'queue', 'worker', 10)
            stalled = self.lua('jobs', int(jid) + 20, 'stalled', 'queue')
            self.assertEqual(len(stalled), int(jid) + 1)
            self.assertEqual(stalled[-1], jid)

    def test_scheduled(self):
        '''Verify that we can get a list of scheduled jobs in a queue'''
        jids = map(str, range(1, 11))
        for jid in jids:
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, jid)
            scheduled = self.lua('jobs', 0, 'scheduled', 'queue')
            self.assertEqual(len(scheduled), int(jid))
            self.assertEqual(scheduled[-1], jid)

    def test_depends(self):
        '''Verify that we can get a list of dependent jobs in a queue'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        jids = map(str, range(0, 10))
        for jid in jids:
            self.lua(
                'put', jid, 'worker', 'queue', jid, 'klass', {}, 0, 'depends', ['a'])
            depends = self.lua('jobs', 0, 'depends', 'queue')
            self.assertEqual(len(depends), int(jid) + 1)
            self.assertEqual(depends[-1], jid)

    def test_recurring(self):
        '''Verify that we can get a list of recurring jobs in a queue'''
        jids = map(str, range(0, 10))
        for jid in jids:
            self.lua(
                'recur', jid, 'queue', jid, 'klass', {}, 'interval', 60, 0)
            recurring = self.lua('jobs', 0, 'recurring', 'queue')
            self.assertEqual(len(recurring), int(jid) + 1)
            self.assertEqual(recurring[-1], jid)

    def test_recurring_offset(self):
        '''Recurring jobs with a future offset should be included'''
        jids = map(str, range(0, 10))
        for jid in jids:
            self.lua(
                'recur', jid, 'queue', jid, 'klass', {}, 'interval', 60, 10)
            recurring = self.lua('jobs', 0, 'recurring', 'queue')
            self.assertEqual(len(recurring), int(jid) + 1)
            self.assertEqual(recurring[-1], jid)

    def test_scheduled_waiting(self):
        '''Jobs that were scheduled but are ready shouldn't be in scheduled'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 10)
        self.assertEqual(len(self.lua('jobs', 20, 'scheduled', 'queue')), 0)

    def test_pagination_complete(self):
        '''Jobs should be able to provide paginated results for complete'''
        jids = map(str, range(100))
        for jid in jids:
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('pop', jid, 'queue', 'worker', 10)
            self.lua('complete', jid, jid, 'worker', 'queue', {})
        # Get two pages and ensure they're what we expect
        jids = list(reversed(jids))
        self.assertEqual(
            self.lua('jobs', 0, 'complete',  0, 50), jids[:50])
        self.assertEqual(
            self.lua('jobs', 0, 'complete', 50, 50), jids[50:])

    def test_pagination_running(self):
        '''Jobs should be able to provide paginated result for running'''
        jids = map(str, range(100))
        self.lua('config.set', 0, 'heartbeat', 1000)
        for jid in jids:
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('pop', jid, 'queue', 'worker', 10)
        # Get two pages and ensure they're what we expect
        self.assertEqual(
            self.lua('jobs', 100, 'running', 'queue',  0, 50), jids[:50])
        self.assertEqual(
            self.lua('jobs', 100, 'running', 'queue', 50, 50), jids[50:])

    def test_scheduled_does_not_acquire_resources(self):
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 1, 'resources', ['r-1'])
        scheduled = self.lua('jobs', 0, 'scheduled', 'queue')

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})

    def test_scheduled_acquires_resources_when_popped(self):
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 1, 'resources', ['r-1'])
        scheduled = self.lua('jobs', 0, 'scheduled', 'queue')

        jobs = self.lua('pop', 1, 'queue', 'worker-1', 1)

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], {})

    def test_scheduled_adds_pending_if_resources_not_available(self):
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 1, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})

        # will trigger scheduled job also, but will not pop
        jobs = self.lua('pop', 1, 'queue', 'worker-1', 1)

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], ['jid-1'])

    def test_scheduled_with_pending_does_pop_when_resources_are_available(self):
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 1, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})

        # will trigger scheduled job also, but will not pop
        jobs = self.lua('pop', 1, 'queue', 'worker-1', 1)

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], ['jid-1'])

        self.lua('complete', 3, 'jid-2', 'worker-1', 'queue', {})

        jobs = self.lua('pop', 1, 'queue', 'worker-1', 1)
        self.assertEqual(len(jobs), 1)
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], {})


class TestQueue(TestQless):
    '''Test queue info tests'''
    expected = {
        'name': 'queue',
        'paused': False,
        'stalled': 0,
        'waiting': 0,
        'running': 0,
        'depends': 0,
        'scheduled': 0,
        'recurring': 0
    }

    def setUp(self):
        TestQless.setUp(self)
        # No grace period
        self.lua('config.set', 0, 'grace-period', 0)

    def test_stalled(self):
        '''Discern stalled job counts correctly'''
        expected = dict(self.expected)
        expected['stalled'] = 1
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        job = self.lua('pop', 1, 'queue', 'worker', 10)[0]
        expires = job['expires'] + 10
        self.assertEqual(self.lua('queues', expires, 'queue'), expected)
        self.assertEqual(self.lua('queues', expires), [expected])

    def test_waiting(self):
        '''Discern waiting job counts correctly'''
        expected = dict(self.expected)
        expected['waiting'] = 1
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

    def test_running(self):
        '''Discern running job counts correctly'''
        expected = dict(self.expected)
        expected['running'] = 1
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

    def test_depends(self):
        '''Discern dependent job counts correctly'''
        expected = dict(self.expected)
        expected['depends'] = 1
        expected['waiting'] = 1
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

    def test_scheduled(self):
        '''Discern scheduled job counts correctly'''
        expected = dict(self.expected)
        expected['scheduled'] = 1
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 10)
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

    def test_recurring(self):
        '''Discern recurring job counts correctly'''
        expected = dict(self.expected)
        expected['recurring'] = 1
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

    def test_recurring_offset(self):
        '''Discern future recurring job counts correctly'''
        expected = dict(self.expected)
        expected['recurring'] = 1
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 10)
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

    def test_pause(self):
        '''Can pause and unpause a queue'''
        jids = map(str, range(10))
        for jid in jids:
            self.lua('put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
        # After pausing, we can't get the jobs, and the state reflects it
        self.lua('pause', 0, 'queue')
        self.assertEqual(len(self.lua('pop', 0, 'queue', 'worker', 100)), 0)
        expected = dict(self.expected)
        expected['paused'] = True
        expected['waiting'] = 10
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

        self.assertTrue(self.lua('paused', 0, 'queue'))

        # Once unpaused, we should be able to pop jobs off
        self.lua('unpause', 0, 'queue')
        self.assertEqual(len(self.lua('pop', 0, 'queue', 'worker', 100)), 10)

        self.assertFalse(self.lua('paused', 0, 'queue'))


    def test_advance(self):
        '''When advancing a job to a new queue, queues should know about it'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('complete', 0, 'jid', 'worker', 'queue', {}, 'next', 'another')
        expected = dict(self.expected)
        expected['name'] = 'another'
        expected['waiting'] = 1
        self.assertEqual(self.lua('queues', 0), [expected, self.expected])

    def test_recurring_move(self):
        '''When moving a recurring job, it should add the queue to queues'''
        expected = dict(self.expected)
        expected['name'] = 'another'
        expected['recurring'] = 1
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 60, 0)
        self.lua('recur.update', 0, 'jid', 'queue', 'another')
        self.assertEqual(self.lua('queues', 0), [expected, self.expected])

    def test_scheduled_waiting(self):
        '''When checking counts, jobs that /were/ scheduled can be waiting'''
        expected = dict(self.expected)
        expected['waiting'] = 1
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 10)
        self.assertEqual(self.lua('queues', 20), [expected])
        self.assertEqual(self.lua('queues', 20, 'queue'), expected)


class TestPut(TestQless):
    '''Test putting jobs into a queue'''
    # For reference:
    #
    #   Put(now, jid, klass, data, delay,
    #       [priority, p],
    #       [tags, t],
    #       [retries, r],
    #       [depends, '[...]'])
    def put(self, *args):
        '''Alias for self.lua('put', ...)'''
        return self.lua('put', *args)

    def test_malformed(self):
        '''Enumerate all the ways in which the input can be messed up'''
        self.assertMalformed(self.put, [
            (12345,),                              # No queue provided
            (12345, 'foo'),                        # No jid provided
            (12345, 'foo', 'bar'),                 # No klass provided
            (12345, 'foo', 'bar', 'whiz'),         # No data provided
            (12345, 'foo', 'bar', 'whiz',
                '{}'),                               # No delay provided
            (12345, 'foo', 'bar', 'whiz',
                '{]'),                               # Malformed data provided
            (12345, 'foo', 'bar', 'whiz',
                '{}', 'number'),                     # Malformed delay provided
            (12345, 'foo', 'bar', 'whiz', '{}', 1,
                'retries'),                          # Retries arg missing
            (12345, 'foo', 'bar', 'whiz', '{}', 1,
                'retries', 'foo'),                   # Retries arg not a number
            (12345, 'foo', 'bar', 'whiz', '{}', 1,
                'tags'),                             # Tags arg missing
            (12345, 'foo', 'bar', 'whiz', '{}', 1,
                'tags', '{]'),                       # Tags arg malformed
            (12345, 'foo', 'bar', 'whiz', '{}', 1,
                'priority'),                         # Priority arg missing
            (12345, 'foo', 'bar', 'whiz', '{}', 1,
                'priority', 'foo'),                  # Priority arg malformed
            (12345, 'foo', 'bar', 'whiz', '{}', 1,
                'depends'),                          # Depends arg missing
            (12345, 'foo', 'bar', 'whiz', '{}', 1,
                'depends', '{]')                     # Depends arg malformed
        ])

    def test_basic(self):
        '''We should be able to put and get jobs'''
        jid = self.lua('put', 12345, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(jid, 'jid')
        # Now we should be able to verify the data we get back
        self.assertEqual(self.lua('get', 12345, 'jid'), {
            'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {},
            'history': [{'q': 'queue', 'what': 'put', 'when': 12345}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'queue',
            'remaining': 5,
            'retries': 5,
            'state': 'waiting',
            'tags': {},
            'tracked': False,
            'resources': {},
            'result_data': {},
            'worker': u''
        })

    def test_data_as_array(self):
        '''We should be able to provide an array as data'''
        # In particular, an empty array should be acceptable, and /not/
        # transformed into a dictionary when it returns
        self.lua('put', 12345, 'worker', 'queue', 'jid', 'klass', [], 0)
        self.assertEqual(self.lua('get', 12345, 'jid')['data'], '[]')

    def test_put_delay(self):
        '''When we put a job with a delay, it's reflected in its data'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 1)
        self.assertEqual(self.lua('get', 0, 'jid')['state'], 'scheduled')
        # After the delay, we should be able to pop
        self.assertEqual(self.lua('pop', 0, 'queue', 'worker', 10), {})
        self.assertEqual(len(self.lua('pop', 2, 'queue', 'worker', 10)), 1)

    def test_put_retries(self):
        '''Reflects changes to 'retries' '''
        self.lua('put', 12345, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 2)
        self.assertEqual(self.lua('get', 12345, 'jid')['retries'], 2)
        self.assertEqual(self.lua('get', 12345, 'jid')['remaining'], 2)

    def test_put_tags(self):
        '''When we put a job with tags, it's reflected in its data'''
        self.lua('put', 12345, 'worker', 'queue', 'jid', 'klass', {}, 0, 'tags', ['foo'])
        self.assertEqual(self.lua('get', 12345, 'jid')['tags'], ['foo'])

    def test_put_priority(self):
        '''When we put a job with priority, it's reflected in its data'''
        self.lua('put', 12345, 'worker', 'queue', 'jid', 'klass', {}, 0, 'priority', 1)
        self.assertEqual(self.lua('get', 12345, 'jid')['priority'], 1)

    def test_put_depends(self):
        '''Dependencies are reflected in job data'''
        self.lua('put', 12345, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 12345, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertEqual(self.lua('get', 12345, 'a')['dependents'], ['b'])
        self.assertEqual(self.lua('get', 12345, 'b')['dependencies'], ['a'])
        self.assertEqual(self.lua('get', 12345, 'b')['state'], 'depends')

    def test_put_depends_with_delay(self):
        '''When we put a job with a depends and a delay it is reflected in the job data'''
        self.lua('put', 12345, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 12345, 'worker', 'queue', 'b', 'klass', {}, 1, 'depends', ['a'])
        self.assertEqual(self.lua('get', 12345, 'a')['dependents'], ['b'])
        self.assertEqual(self.lua('get', 12345, 'b')['dependencies'], ['a'])
        self.assertEqual(self.lua('get', 12345, 'b')['state'], 'depends')

    def test_move(self):
        '''Move is described in terms of puts.'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {'foo': 'bar'}, 0)
        self.lua('put', 0, 'worker', 'other', 'jid', 'klass', {'foo': 'bar'}, 0)
        self.assertEqual(self.lua('get', 1, 'jid'), {
            'data': '{"foo": "bar"}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {},
            'history': [
                {'q': 'queue', 'what': 'put', 'when': 0},
                {'q': 'other', 'what': 'put', 'when': 0}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'other',
            'remaining': 5,
            'retries': 5,
            'state': 'waiting',
            'tags': {},
            'tracked': False,
            'resources': {},
            'result_data': {},
            'worker': u''})

    def test_move_update(self):
        '''When moving, ensure data's only changed when overridden'''
        for key, value, update in [
            ('priority', 1, 2),
            ('tags', ['foo'], ['bar']),
            ('retries', 2, 3)]:
            # First, when not overriding the value, it should stay the sam3
            # even after moving
            self.lua('put', 0, 'worker', 'queue', key, 'klass', {}, 0, key, value)
            self.lua('put', 0, 'worker', 'other', key, 'klass', {}, 0)
            self.assertEqual(self.lua('get', 0, key)[key], value)
            # But if we override it, it should be updated
            self.lua('put', 0, 'worker', 'queue', key, 'klass', {}, 0, key, update)
            self.assertEqual(self.lua('get', 0, key)[key], update)

        # Updating dependecies has to be special-cased a little bit. Without
        # overriding dependencies, they should be carried through the move
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'c', 'klass', {}, 0, 'depends', ['a'])
        self.lua('put', 0, 'worker', 'other', 'c', 'klass', {}, 0)
        self.assertEqual(self.lua('get', 0, 'a')['dependents'], ['c'])
        self.assertEqual(self.lua('get', 0, 'b')['dependents'], {})
        self.assertEqual(self.lua('get', 0, 'c')['dependencies'], ['a'])
        # But if we move and update depends, then it should correctly reflect
        self.lua('put', 0, 'worker', 'queue', 'c', 'klass', {}, 0, 'depends', ['b'])
        self.assertEqual(self.lua('get', 0, 'a')['dependents'], {})
        self.assertEqual(self.lua('get', 0, 'b')['dependents'], ['c'])
        self.assertEqual(self.lua('get', 0, 'c')['dependencies'], ['b'])

    def test_resources(self):
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('pop', 1, 'queue', 'worker-1', 1)
        self.assertEquals(len(res), 1)
        job = res[0]
        self.assertEquals(job['jid'], 'jid-1')

        res = self.lua('pop', 1, 'queue', 'worker-2', 1)
        self.assertEquals(len(res), 0)

        res = self.lua('complete', 10, job['jid'], 'worker-1', 'queue', {})
        self.assertEquals(res, 'complete')

        res = self.lua('pop', 1, 'queue', 'worker-2', 1)
        self.assertEquals(len(res), 1)
        job = res[0]
        self.assertEquals(job['jid'], 'jid-2')

        res = self.lua('complete', 10, job['jid'], 'worker-2', 'queue', {})
        self.assertEquals(res, 'complete')

    def test_does_replace_when_not_running(self):
        res = self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0)
        self.assertEqual(res, 'jid-1')

        res = self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0)
        self.assertEqual(res, 'jid-1')

    def test_no_replace_when_running_and_not_expired(self):
        res = self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0)
        self.assertEqual(res, 'jid-1')

        res = self.lua('pop', 1, 'queue', 'worker-1', 1)
        self.assertEqual(res[0]['jid'], 'jid-1')

        res = self.lua('put', 5, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0)
        self.assertEqual(res, 56)

    def test_default_is_to_replace_existing_job(self):
        res = self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0)
        self.assertEqual(res, 'jid-1')

        res = self.lua('pop', 1, 'queue', 'worker-1', 1)
        self.assertEqual(res[0]['jid'], 'jid-1')

        res = self.lua('put', 5, None, 'queue', 'jid-1', 'klass', {}, 0)
        self.assertEqual(res, 'jid-1')

    def test_does_replace_when_running_and_expired(self):
        res = self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0)
        self.assertEqual(res, 'jid-1')

        res = self.lua('pop', 1, 'queue', 'worker-1', 1)
        self.assertEqual(res[0]['jid'], 'jid-1')

        res = self.lua('put', 65, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0)
        self.assertEqual(res, 'jid-1')

    def test_put_with_interval_throttles_consecutive_jobs(self):
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0, 'interval', 10)
        self.lua('pop', 1, 'queue', 'worker-1', 1)
        self.lua('complete', 5, 'jid-1', 'worker-1', 'queue', {})

        res = self.lua('put', 6, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0, 'interval', 10)
        self.assertEqual('jid-1', res)
        res = self.lua('put', 7, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0, 'interval', 10)
        self.assertEqual('jid-1', res)

        res = self.lua('pop', 7, 'queue', 'worker-1', 1)
        self.assertDictEqual(res, {})

        res = self.lua('pop', 15, 'queue', 'worker-1', 1)
        self.assertEqual(res[0]['jid'], 'jid-1')

    def test_put_with_changed_interval_allows_immediate_consecutive_jobs(self):
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0, 'interval', 10)
        self.lua('pop', 1, 'queue', 'worker-1', 1)
        self.lua('complete', 5, 'jid-1', 'worker-1', 'queue', {})

        res = self.lua('put', 6, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0, 'interval', 10)
        self.assertEqual('jid-1', res)
        res = self.lua('pop', 7, 'queue', 'worker-1', 1)
        self.assertDictEqual(res, {})

        res = self.lua('put', 8, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0, 'interval', 0)
        self.assertEqual('jid-1', res)

        res = self.lua('pop', 7, 'queue', 'worker-1', 1)
        self.assertEqual(res[0]['jid'], 'jid-1')

    def test_put_with_interval_enforces_throttling_on_subsequent_jobs_when_omitted(self):
        """This test ensures subsequent put calls without interval continue to enforce
          throttling for that job
        """
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0, 'interval', 10)
        self.lua('pop', 1, 'queue', 'worker-1', 1)
        self.lua('complete', 5, 'jid-1', 'worker-1', 'queue', {})

        res = self.lua('put', 6, None, 'queue', 'jid-1', 'klass', {}, 0, 'replace', 0)
        self.assertEqual('jid-1', res)
        res = self.lua('pop', 7, 'queue', 'worker-1', 1)
        self.assertDictEqual(res, {})

        res = self.lua('pop', 15, 'queue', 'worker-1', 1)
        self.assertEqual(res[0]['jid'], 'jid-1')






class TestPeek(TestQless):
    '''Test peeking jobs'''
    # For reference:
    #
    #   QlessAPI.peek = function(now, queue, count)
    def test_malformed(self):
        '''Enumerate all the ways in which the input can be malformed'''
        self.assertMalformed(self.lua, [
            ('peek', 12345,),                         # No queue provided
            ('peek', 12345, 'foo'),                   # No count provided
            ('peek', 12345, 'foo', 'number'),         # Count arg malformed
        ])

    def test_basic(self):
        '''Can peek at a single waiting job'''
        # No jobs for an empty queue
        self.assertEqual(self.lua('peek', 0, 'foo', 10), {})
        self.lua('put', 0, 'worker', 'foo', 'jid', 'klass', {}, 0)
        # And now we should see a single job
        self.assertEqual(self.lua('peek', 1, 'foo', 10), [{
            'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {},
            'history': [{'q': 'foo', 'what': 'put', 'when': 0}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'foo',
            'remaining': 5,
            'retries': 5,
            'state': 'waiting',
            'tags': {},
            'tracked': False,
            'resources': {},
            'result_data': {},
            'worker': u''
        }])
        # With several jobs in the queue, we should be able to see more
        self.lua('put', 2, 'worker', 'foo', 'jid2', 'klass', {}, 0)
        self.assertEqual([o['jid'] for o in self.lua('peek', 3, 'foo', 10)], [
            'jid', 'jid2'])

    def test_priority(self):
        '''Peeking honors job priorities'''
        # We'll inserts some jobs with different priorities
        for jid in xrange(-10, 10):
            self.lua(
                'put', 0, 'worker', 'queue', jid, 'klass', {}, 0, 'priority', jid)

        # Peek at the jobs, and they should be in the right order
        jids = [job['jid'] for job in self.lua('peek', 1, 'queue', 100)]
        self.assertEqual(jids, map(str, range(9, -11, -1)))

    def test_time_order(self):
        '''Honor the time that jobs were put, priority constant'''
        # Put 100 jobs on with different times
        for time in xrange(100):
            self.lua('put', time, 'worker', 'queue', time, 'klass', {}, 0)
        jids = [job['jid'] for job in self.lua('peek', 200, 'queue', 100)]
        self.assertEqual(jids, map(str, range(100)))

    def test_move(self):
        '''When we move a job, it should be visible in the new, not old'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'other', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('peek', 1, 'queue', 10), {})
        self.assertEqual(self.lua('peek', 1, 'other', 10)[0]['jid'], 'jid')

    def test_recurring(self):
        '''We can peek at recurring jobs'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {}, 'interval', 10, 0)
        self.assertEqual(len(self.lua('peek', 99, 'queue', 100)), 10)

    def test_priority_update(self):
        '''We can change a job's priority'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0, 'priority', 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'priority', 1)
        self.assertEqual(['b', 'a'],
            [j['jid'] for j in self.lua('peek', 0, 'queue', 100)])
        self.lua('priority', 0, 'a', 2)
        self.assertEqual(['a', 'b'],
            [j['jid'] for j in self.lua('peek', 0, 'queue', 100)])

    def test_can_queue_and_peek_multiples(self):
        self.lua('resource.set', 0, 'r-1', 2)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('peek', 99, 'queue', 5)
        self.assertEqual(len(res), 2)

    def test_can_peek_returns_correct_number_for_limited_resource(self):
        self.lua('resource.set', 0, 'r-1', 2)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-3', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('peek', 99, 'queue', 5)
        self.assertEqual(len(res), 2)



class TestPop(TestQless):
    '''Test popping jobs'''
    # For reference:
    #
    #   QlessAPI.pop = function(now, queue, worker, count)
    def test_malformed(self):
        '''Enumerate all the ways this can be malformed'''
        self.assertMalformed(self.lua, [
            ('pop', 12345,),                              # No queue provided
            ('pop', 12345, 'queue'),                      # No worker provided
            ('pop', 12345, 'queue', 'worker'),            # No count provided
            ('pop', 12345, 'queue', 'worker', 'number'),  # Malformed count
        ])

    def test_basic(self):
        '''Pop some jobs in a simple way'''
        # If the queue is empty, you get no jobs
        self.assertEqual(self.lua('pop', 0, 'queue', 'worker', 10), {})
        # With job put, we can get one back
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('pop', 1, 'queue', 'worker', 1), [{
            'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 61,
            'failure': {},
            'history': [{'q': 'queue', 'what': 'put', 'when': 0},
                {'what': 'popped', 'when': 1, 'worker': 'worker'}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'queue',
            'remaining': 5,
            'retries': 5,
            'state': 'running',
            'tags': {},
            'tracked': False,
            'resources': {},
            'result_data': {},
            'worker': 'worker'}])

    def test_pop_many(self):
        '''We should be able to pop off many jobs'''
        for jid in range(10):
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
        # This should only pop the first 7
        self.assertEqual(
            [job['jid'] for job in self.lua('pop', 100, 'queue', 'worker', 7)],
            map(str, range(7)))
        # This should only leave 3 left
        self.assertEqual(
            [job['jid'] for job in self.lua('pop', 100, 'queue', 'worker', 10)],
            map(str, range(7, 10)))

    def test_priority(self):
        '''Popping should honor priority'''
        # We'll inserts some jobs with different priorities
        for jid in xrange(-10, 10):
            self.lua(
                'put', 0, 'worker', 'queue', jid, 'klass', {}, 0, 'priority', jid)

        # Peek at the jobs, and they should be in the right order
        jids = [job['jid'] for job in self.lua('pop', 1, 'queue', 'worker', 100)]
        self.assertEqual(jids, map(str, range(9, -11, -1)))

    def test_time_order(self):
        '''Honor the time jobs were inserted, priority held constant'''
        # Put 100 jobs on with different times
        for time in xrange(100):
            self.lua('put', time, 'worker', 'queue', time, 'klass', {}, 0)
        jids = [job['jid'] for job in self.lua('pop', 200, 'queue', 'worker', 100)]
        self.assertEqual(jids, map(str, range(100)))

    def test_move(self):
        '''When we move a job, it should be visible in the new, not old'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'other', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('pop', 1, 'queue', 'worker', 10), {})
        self.assertEqual(self.lua('pop', 1, 'other', 'worker', 10)[0]['jid'], 'jid')

    def test_max_concurrency(self):
        '''We can control the maxinum number of jobs available in a queue'''
        self.lua('config.set', 0, 'queue-max-concurrency', 5)
        for jid in xrange(10):
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
        self.assertEqual(len(self.lua('pop', 10, 'queue', 'worker', 10)), 5)
        # But as we complete the jobs, we can pop more
        for jid in xrange(5):
            self.lua('complete', 10, jid, 'worker', 'queue', {})
            self.assertEqual(
                len(self.lua('pop', 10, 'queue', 'worker', 10)), 1)

    def test_reduce_max_concurrency(self):
        '''We can reduce max_concurrency at any time'''
        # We'll put and pop a bunch of jobs, then restruct concurrency and
        # validate that jobs can't be popped until we dip below that level
        for jid in xrange(100):
            self.lua('put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
        self.lua('pop', 100, 'queue', 'worker', 10)
        self.lua('config.set', 100, 'queue-max-concurrency', 5)
        for jid in xrange(6):
            self.assertEqual(
                len(self.lua('pop', 100, 'queue', 'worker', 10)), 0)
            self.lua('complete', 100, jid, 'worker', 'queue', {})
        # And now we should be able to start popping jobs
        self.assertEqual(
            len(self.lua('pop', 100, 'queue', 'worker', 10)), 1)

    def test_stalled_max_concurrency(self):
        '''Stalled jobs can still be popped with max concurrency'''
        self.lua('config.set', 0, 'queue-max-concurrency', 1)
        self.lua('config.set', 0, 'grace-period', 0)
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 5)
        job = self.lua('pop', 0, 'queue', 'worker', 10)[0]
        job = self.lua('pop', job['expires'] + 10, 'queue', 'worker', 10)[0]
        self.assertEqual(job['jid'], 'jid')
        self.assertEqual(job['remaining'], 4)

    def test_fail_max_concurrency(self):
        '''Failing a job makes space for a job in a queue with concurrency'''
        self.lua('config.set', 0, 'queue-max-concurrency', 1)
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 1, 'worker', 'queue', 'b', 'klass', {}, 0)
        self.lua('pop', 2, 'queue', 'worker', 10)
        self.lua('fail', 3, 'a', 'worker', 'group', 'message', {})
        job = self.lua('pop', 4, 'queue', 'worker', 10)[0]
        self.assertEqual(job['jid'], 'b')


class TestResources(TestQless):
    """Queues should correctly handle jobs that require resources"""

    def test_single_resource_does_not_pop_when_in_use(self):
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('pop', 1, 'queue', 'worker-1', 1)
        self.assertEquals(len(res), 1)
        job = res[0]
        self.assertEquals(job['jid'], 'jid-1')

        res = self.lua('pop', 1, 'queue', 'worker-2', 1)
        self.assertEquals(len(res), 0)

        res = self.lua('complete', 10, job['jid'], 'worker-1', 'queue', {})
        self.assertEquals(res, 'complete')

        res = self.lua('pop', 1, 'queue', 'worker-2', 1)
        self.assertEquals(len(res), 1)
        job = res[0]
        self.assertEquals(job['jid'], 'jid-2')

        res = self.lua('complete', 10, job['jid'], 'worker-2', 'queue', {})
        self.assertEquals(res, 'complete')

    def test_recurring_job_can_consume_resources(self):
        self.lua('resource.set', 0, 'r-1', 1)

        res = self.lua('recur', 0, 'queue', 'jid-1', 'klass', {}, 'interval', 60, 0, 'resources', ['r-1'])

        res = self.lua('pop', 15, 'queue', 'worker-1', 1)

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1-1'])
        self.assertEqual(res['pending'], {})

    def test_recurring_job_waits_for_resources(self):
        """Job recurs, waits for resource to become available"""

        self.lua('resource.set', 0, 'r-1', 1)

        res = self.lua('recur', 0, 'queue', 'jid-1', 'klass', {}, 'interval', 60, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('pop', 15, 'queue', 'worker-1', 5)

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], ['jid-1-1'])

    def test_higher_priority_job_acquires_resource_first(self):
        """Jobs with higher priority received resource lock before lower priority"""

        self.lua('resource.set', 0, 'r-1', 1)

        self.lua('put', 10, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])

        self.lua('put', 15, None, 'queue', 'jid-low', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 15, None, 'queue', 'jid-high', 'klass', {}, 0, 'resources', ['r-1'], 'priority', 5)

        res = self.lua('pop', 16, 'queue', 'worker-1', 1)
        self.lua('complete', 17, 'jid-1', 'worker-1', 'queue', {})

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-high'])
        self.assertEqual(res['pending'], ['jid-low'])

class TestMultipleResources(TestQless):
    """Queues should correctly handle jobs that require multiple resources"""
    def test_job_acquires_multiple_and_releases(self):
        """Jobs with multiple resources acquire both and release both"""
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('resource.set', 0, 'r-2', 1)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1','r-2'])

        #res = self.lua('pop', 15, 'queue', 'worker-1', 1)

        #self.assertEquals(len(res), 1)
        #job = res[0]
        #self.assertEquals(job['jid'], 'jid-1')

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.get', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], {})

        res = self.lua('pop', 15, 'queue', 'worker-1', 1)
        self.lua('complete', 17, 'jid-1', 'worker-1', 'queue', {})

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.get', 0, 'r-2')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})

    def test_job_waits_on_one_of_multiple(self):

        """Jobs with multiple resources waits on one"""
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('resource.set', 0, 'r-2', 1)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 1, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1','r-2'])

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], ['jid-2'])

        res = self.lua('resource.get', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})

        #pop and complete the first job and the second one should get the other resource
        res = self.lua('pop', 15, 'queue', 'worker-1', 1)
        self.lua('complete', 17, 'jid-1', 'worker-1', 'queue', {})

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.get', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})

        #pop and complete the second job
        res = self.lua('pop', 15, 'queue', 'worker-1', 1)
        self.lua('complete', 17, 'jid-2', 'worker-1', 'queue', {})

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.get', 0, 'r-2')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})

    def test_job_waits_on_multiple(self):
        """Jobs with multiple resources waits on multiple"""
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('resource.set', 0, 'r-2', 1)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1','r-2'])
        self.lua('put', 1, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1','r-2'])

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], ['jid-2'])

        res = self.lua('resource.get', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], ['jid-2'])

         #pop and complete the second job
        res = self.lua('pop', 15, 'queue', 'worker-1', 1)
        self.lua('complete', 17, 'jid-1', 'worker-1', 'queue', {})

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.get', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})

    def test_job_multiple_limit(self):
        """Jobs with multiple resources and at limit on one"""
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('resource.set', 0, 'r-2', 2)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1','r-2'])
        self.lua('put', 1, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1','r-2'])

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], ['jid-2'])

        res = self.lua('resource.get', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-1','jid-2'])
        self.assertEqual(res['pending'], {})

        #pop and complete the second job
        res = self.lua('pop', 15, 'queue', 'worker-1', 1)
        self.lua('complete', 17, 'jid-1', 'worker-1', 'queue', {})

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.get', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})

    def test_job_multiple_wait_on_singles(self):
        """Jobs with multiple resources waits on jobs with single resources"""
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('resource.set', 0, 'r-2', 1)

        #finish this test.  Then add code to change resource count up and down
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 1, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-2'])
        self.lua('put', 2, None, 'queue', 'jid-3', 'klass', {}, 0, 'resources', ['r-1','r-2'])
        self.lua('put', 3, None, 'queue', 'jid-4', 'klass', {}, 0, 'resources', ['r-1'])

        # jobs 3 and 4 are witing on r-1
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], ['jid-3','jid-4'])

        #job 3 is waiting on r-2
        res = self.lua('resource.get', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], ['jid-3'])

        #pop the job.  It should be jid-1
        res = self.lua('pop', 15, 'queue', 'worker-1', 1)
        self.assertEqual(self.lua('workers', 16, 'worker-1'), {
            'jobs': ['jid-1'],
            'stalled': {}
        })

        self.lua('complete', 16, 'jid-1', 'worker-1', 'queue', {})

        #pop again
        res = self.lua('pop', 17, 'queue', 'worker-1', 1)
        #r-1 is released, but jid-4 will have to get r-1 after jid-3
        self.assertEqual(self.lua('workers', 18, 'worker-1'), {
            'jobs': ['jid-2'],
            'stalled': {}
        })

        # jobs 3 has the lock on r-1, job 4 is pending
        res = self.lua('resource.get', 19, 'r-1')
        self.assertEqual(res['locks'], ['jid-3'])
        self.assertEqual(res['pending'], ['jid-4'])

        #job 3 is waiting on r-1
        res = self.lua('resource.get', 19, 'r-2')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], ['jid-3'])

        #complete jid-2
        self.lua('complete', 19, 'jid-2', 'worker-1', 'queue', {})

        #pop again
        res = self.lua('pop', 20, 'queue', 'worker-1', 1)

        #test to make sure job-3 is not on a worker
        self.assertEqual(self.lua('workers', 20, 'worker-1'), {
            'jobs': ['jid-3'],
            'stalled': {}
        })

         # jobs 3 has the lock on r-1, job 4 is pending
        res = self.lua('resource.get', 20, 'r-1')
        self.assertEqual(res['locks'], ['jid-3'])
        self.assertEqual(res['pending'], ['jid-4'])

        #job 3 has the lock on r-2
        res = self.lua('resource.get', 20, 'r-2')
        self.assertEqual(res['locks'], ['jid-3'])
        self.assertEqual(res['pending'], {})

         #complete jid-3
        self.lua('complete', 21, 'jid-3', 'worker-1', 'queue', {})

        #pop again
        res = self.lua('pop', 21, 'queue', 'worker-1', 1)

        self.assertEqual(self.lua('workers', 21, 'worker-1'), {
            'jobs': ['jid-4'],
            'stalled': {}
        })

        # jobs 4 has the lock on r-1
        res = self.lua('resource.get', 21, 'r-1')
        self.assertEqual(res['locks'], ['jid-4'])
        self.assertEqual(res['pending'], {})

        #r-2 is empty
        res = self.lua('resource.get', 21, 'r-2')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})















