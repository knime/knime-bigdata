from typing import Dict
import knime.extension.ports as kp
import knime.api.schema as ks


class DatabricksWorkspacePortObjectSpec(ks.CredentialPortObjectSpec):
    """Spec of a Databricks Workspace.
    Can be used to connect and authenticate to a Databricks Workspace."""

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
    """Databricks Workspace port object.
    All the necessary information can be accessed via the spec."""

    def __init__(self, spec: DatabricksWorkspacePortObjectSpec):
        self._spec = spec

    @property
    def spec(self) -> DatabricksWorkspacePortObjectSpec:
        return self._spec


class DatabricksWorkspacePortConverter(
    kp.PortObjectDecoder[
        DatabricksWorkspacePortObject,
        kp.EmptyIntermediateRepresentation,
        DatabricksWorkspacePortObjectSpec,
        kp.StringIntermediateRepresentation,
    ]
):
    """Python decoder for Databricks Workspaces passed via KNIME ports."""

    def __init__(self):
        super().__init__(
            DatabricksWorkspacePortObject, DatabricksWorkspacePortObjectSpec
        )

    def decode_object(
        self, intermediate_representation, spec
    ) -> DatabricksWorkspacePortObject:
        return DatabricksWorkspacePortObject(spec)

    def decode_spec(
        self, intermediate_representation
    ) -> DatabricksWorkspacePortObjectSpec:
        import json

        data = json.loads(intermediate_representation.getStringRepresentation())
        return DatabricksWorkspacePortObjectSpec(
            data["data"], data.get("workspace_url")
        )
