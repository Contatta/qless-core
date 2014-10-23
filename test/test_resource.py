"""Test the queue functionality"""

from common import TestQless


class TestResources(TestQless):
    """We should be able to access resources"""

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

    def test_malformed(self):
        self.assertMalformed(self.lua, [
            ('resource.set', 0),
            ('resource.set', 0, 'test'),
            ('resource.set', 0, 'test', 'sfdgl'),
            ('resource.data', 0),
            ('resource.unset', 0),
            ('resource.locks', 0),
        ])

    def test_set(self):
        res = self.lua('resource.set', 0, 'test', 5)

        self.assertEquals(res, 'test')

    def test_get(self):
        self.lua('resource.set', 0, 'test', 5)
        res = self.lua('resource.get', 0, 'test')

        self.assertEquals(res, 5)

    def test_does_not_exist(self):
        res = self.lua('resource.exists', 0, 'test')

        self.assertFalse(res)

    def test_does_exist(self):
        self.lua('resource.set', 0, 'test', 5)
        res = self.lua('resource.exists', 0, 'test')

        self.assertTrue(res)

    def test_unset(self):
        self.lua('resource.set', 0, 'test', 5)
        self.assertIsInstance(self.lua('resource.data', 0, 'test'), dict)
        self.lua('resource.unset', 0, 'test')
        self.assertIsNone(self.lua('resource.data', 0, 'test'))

    def test_lock_count(self):
        self.lua('resource.set', 0, 'test', 5)
        locks = self.lua('resource.lock_count', 0, 'test')
        self.assertEquals(locks, 0)

    def test_pending_count(self):
        self.lua('resource.set', 0, 'test', 5)
        locks = self.lua('resource.pending_count', 0, 'test')
        self.assertEquals(locks, 0)

    def test_does_add_lock_and_pending(self):
        self.lua('resource.set', 0, 'r-1', 1)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])

        locks = self.lua('resource.locks', 0, 'r-1')
        pending = self.lua('resource.pending', 0, 'r-1')

        self.assertEqual(locks, ['jid-1'])
        self.assertEqual(pending, ['jid-2'])

    def test_resource_set_increase(self):
        self.lua('resource.set', 0, 'r-1', 0)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])

        locks = self.lua('resource.locks', 0, 'r-1')
        pending = self.lua('resource.pending', 0, 'r-1')

        self.assertEqual(locks, {})
        self.assertEqual(pending, ['jid-2','jid-1'])

        self.lua('resource.set', 0, 'r-1', 1)
        locks = self.lua('resource.locks', 0, 'r-1')
        pending = self.lua('resource.pending', 0, 'r-1')

        self.assertEqual(locks, ['jid-2'])
        self.assertEqual(pending, ['jid-1'])

    def test_lock_and_pending_on_recur(self):

        self.lua('resource.set', 0, 'r-1', 0)

        self.lua('recur', 0, 'queue', 'jid-1', 'klass', {}, 'interval', 60, 0, 'resources', ['r-1'])
        self.lua('recur', 0, 'queue', 'jid-2', 'klass', {}, 'interval', 60, 0, 'resources', ['r-1'])

        popped = self.lua('pop', 0, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 0)

        locks = self.lua('resource.locks', 0, 'r-1')
        pending = self.lua('resource.pending', 0, 'r-1')

        self.assertEqual(locks, {})
        self.assertEqual(pending, ['jid-2-1','jid-1-1'])

        self.lua('resource.set', 1, 'r-1', 1)

        locks = self.lua('resource.locks', 0, 'r-1')
        pending = self.lua('resource.pending', 0, 'r-1')

        self.assertEqual(locks, ['jid-2-1'])
        self.assertEqual(pending, ['jid-1-1'])

        popped = self.lua('pop', 2, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 1)
        self.assertEqual(popped[0]['jid'], 'jid-2-1')


    def test_does_not_add_lock_and_pending(self):
        self.lua('resource.set', 0, 'r-1', 1)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('resource.data', 0, 'r-1')

        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], {})

    def test_increase_resource_max(self):
        """Jobs waiting on resources get added to the queue when the resource max is increased"""
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('resource.set', 0, 'r-2', 1)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1','r-2'])
        self.lua('put', 1, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1','r-2'])

        res = self.lua('resource.data', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], ['jid-2'])

        res = self.lua('resource.data', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], ['jid-2'])

        expected = dict(self.expected)
        expected['waiting'] = 1
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

        # increase the resources and should get the locks
        self.lua('resource.set', 0, 'r-1', 2)

        res = self.lua('resource.data', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1','jid-2'])
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.data', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], ['jid-2'])

        #still one waiting
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

        #then increase the other resource and both should be waiting on the queue
        self.lua('resource.set', 0, 'r-2', 2)
        res = self.lua('resource.data', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1','jid-2'])
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.data', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-1','jid-2'])
        self.assertEqual(res['pending'], {})

        #check that job got on the queue when it got the needed resources
        expected['waiting'] = 2
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

    def test_decrease_resource_max(self):
        """Jobs waiting on resources don't get run when the resource max has been decreased"""
        self.lua('resource.set', 0, 'r-1', 2)
        self.lua('resource.set', 0, 'r-2', 2)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1','r-2'])
        self.lua('put', 1, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1','r-2'])
        self.lua('put', 2, None, 'queue', 'jid-3', 'klass', {}, 0, 'resources', ['r-1','r-2'])

        res = self.lua('resource.data', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1','jid-2'])
        self.assertEqual(res['pending'], ['jid-3'])

        res = self.lua('resource.data', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-1','jid-2'])
        self.assertEqual(res['pending'], ['jid-3'])

        expected = dict(self.expected)
        expected['waiting'] = 2
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

        #descease to 0, has no impact on current jobs, but pending jobs don't get started on complete.
        self.lua('resource.set', 0, 'r-2', 0)

        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

        res = self.lua('pop', 3, 'queue', 'worker-1', 1)
        self.assertEqual(self.lua('workers', 4, 'worker-1'), {
            'jobs': ['jid-1'],
            'stalled': {}
        })

        self.lua('complete', 4, 'jid-1', 'worker-1', 'queue', {})

        #verify jid-3 still pending on r-2 but gets r-1
        res = self.lua('resource.data', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2','jid-3'])
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.data', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], ['jid-3'])

        expected['waiting'] = 1
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

        #now complete the other job and make sure jid-3 doesn't get r-2 and doesn't get on queue
        res = self.lua('pop', 5, 'queue', 'worker-1', 1)
        self.assertEqual(self.lua('workers', 5, 'worker-1'), {
            'jobs': ['jid-2'],
            'stalled': {}
        })

        self.lua('complete', 5, 'jid-2', 'worker-1', 'queue', {})

        res = self.lua('resource.data', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-3'])
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.data', 0, 'r-2')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], ['jid-3'])

        expected['waiting'] = 0
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])

        #now finally increase r-2 to 1 and verify job gets picked up.
        self.lua('resource.set', 6, 'r-2', 1)

        res = self.lua('resource.data', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-3'])
        self.assertEqual(res['pending'], {})

        res = self.lua('resource.data', 0, 'r-2')
        self.assertEqual(res['locks'], ['jid-3'])
        self.assertEqual(res['pending'], {})

        expected['waiting'] = 1
        self.assertEqual(self.lua('queues', 0, 'queue'), expected)
        self.assertEqual(self.lua('queues', 0), [expected])























