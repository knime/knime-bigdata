from typing import Dict
import knime.extension.ports as kp
import knime.api.schema as ks


class DatabricksWorkspacePortObjectSpec(ks.CredentialPortObjectSpec):
    def __init__(self, xml_data, workspace_url):
        super().__init__(xml_data)
        self._workspace_url = workspace_url

    @property
    def workspace_url(self):
        return self._workspace_url

    def serialize(self):
        data = super().serialize()
        data["workspace_url"] = self._workspace_url
        return data

    @classmethod
    def deserialize(cls, data: Dict):
        return cls(data["data"], data.get("workspace_url"))


class DatabricksWorkspacePortObject:
    def __init__(self, spec: DatabricksWorkspacePortObjectSpec):
        self._spec = spec

    @property
    def spec(self) -> DatabricksWorkspacePortObjectSpec:
        return self._spec


class DatabricksWorkspacePortConverter(
    kp.KnimeToPyPortObjectConverter[
        DatabricksWorkspacePortObject,
        kp.NonePythonTransfer,
        DatabricksWorkspacePortObjectSpec,
        kp.StringPythonTransfer,
    ]
):
    def __init__(self):
        super().__init__(
            DatabricksWorkspacePortObject, DatabricksWorkspacePortObjectSpec
        )

    def convert_obj_to_python(
        self, transfer, spec, port_info
    ) -> DatabricksWorkspacePortObject:
        return DatabricksWorkspacePortObject(spec)

    def convert_spec_to_python(
        self, transfer, port_info
    ) -> DatabricksWorkspacePortObjectSpec:
        import json

        data = json.loads(transfer.getStringRepresentation())
        return DatabricksWorkspacePortObjectSpec(
            data["data"], data.get("workspace_url")
        )
