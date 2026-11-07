import pathlib

import pytest

from jobs import conform


def test_a_staged_directory_with_no_enabled_source_is_refused(tmp_path: pathlib.Path):
    (tmp_path / 'austin.yml').write_text('source_id: animal.austin\nenabled: false\n')

    with pytest.raises(conform.NoEnabledSourceError):
        conform.load_source_specs(tmp_path)
