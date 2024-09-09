from dags.sla113.sla_113_dag_impl import get_ids_to_delete_impl
from mock import patch, MagicMock


class TestSLA113MySQL_dag:
    @patch('dags.sla113.sla_113_dag_impl.open')
    @patch('dags.sla113.sla_113_dag_impl.json.load')
    def test_get_ids_to_delete_impl_max_3(self, json_load_mock, open_mock):
        ids_to_delete_file = 'test_file'
        max_delete_rows = 3
        open_mock.return_value = MagicMock()
        json_load_mock.return_value = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        x = get_ids_to_delete_impl(ids_to_delete_file, max_delete_rows)

        open_mock.assert_called_once_with(ids_to_delete_file)
        json_load_mock.assert_called_once_with(open_mock.return_value.__enter__())
        assert x == [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    @patch('dags.sla113.sla_113_dag_impl.open')
    @patch('dags.sla113.sla_113_dag_impl.json.load')
    def test_get_ids_to_delete_impl_max_2(self, json_load_mock, open_mock):
        ids_to_delete_file = 'test_file'
        max_delete_rows = 2
        open_mock.return_value = MagicMock()
        json_load_mock.return_value = [1, 2, 3, 4, 5, 6]
        x = get_ids_to_delete_impl(ids_to_delete_file, max_delete_rows)

        open_mock.assert_called_once_with(ids_to_delete_file)
        json_load_mock.assert_called_once_with(open_mock.return_value.__enter__())
        assert x == [[1, 2], [3, 4], [5, 6]]

    @patch('dags.sla113.sla_113_dag_impl.open')
    @patch('dags.sla113.sla_113_dag_impl.json.load')
    def test_get_ids_to_delete_impl_max_empty(self, json_load_mock, open_mock):
        ids_to_delete_file = 'test_file'
        max_delete_rows = 3
        open_mock.return_value = MagicMock()
        json_load_mock.return_value = []
        x = get_ids_to_delete_impl(ids_to_delete_file, max_delete_rows)

        open_mock.assert_called_once_with(ids_to_delete_file)
        json_load_mock.assert_called_once_with(open_mock.return_value.__enter__())
        assert x == []

    @patch('dags.sla113.sla_113_dag_impl.open')
    @patch('dags.sla113.sla_113_dag_impl.json.load')
    def test_get_ids_to_delete_impl_max_non_divisible(self, json_load_mock, open_mock):
        ids_to_delete_file = 'test_file'
        max_delete_rows = 4
        open_mock.return_value = MagicMock()
        json_load_mock.return_value = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        x = get_ids_to_delete_impl(ids_to_delete_file, max_delete_rows)

        open_mock.assert_called_once_with(ids_to_delete_file)
        json_load_mock.assert_called_once_with(open_mock.return_value.__enter__())
        assert x == [[1, 2, 3, 4], [5, 6, 7, 8], [9]]
