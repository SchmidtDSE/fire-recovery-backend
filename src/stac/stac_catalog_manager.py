from typing import Dict, List, Any, Optional
import pystac
from src.core.storage.interface import StorageInterface
from src.stac.stac_item_factory import STACItemFactory
from datetime import datetime
from geojson_pydantic import Polygon, Feature


class STACCatalogManager:
    """Manager class for creating and maintaining a proper STAC catalog hierarchy"""

    def __init__(self, base_url: str, storage: StorageInterface):
        """
        Initialize the STAC catalog manager

        Args:
            base_url: Base URL for the catalog
            storage: Storage interface for persisting catalog files
        """
        self.base_url = base_url
        self.storage = storage
        self.item_factory = STACItemFactory(base_url)

        # Collection definitions
        self.collections = {
            "fire-severity": {
                "id": "fire-severity",
                "title": "Fire Severity Analysis",
                "description": "Fire severity analysis products including RBR, dNBR, and RdNBR indices",
                "keywords": ["fire", "severity", "burn", "ratio", "remote-sensing"],
            },
            "fire-boundaries": {
                "id": "fire-boundaries",
                "title": "Fire Boundaries",
                "description": "Fire perimeter boundaries from satellite analysis",
                "keywords": ["fire", "boundary", "perimeter", "geojson"],
            },
            "vegetation-matrices": {
                "id": "vegetation-matrices",
                "title": "Vegetation Fire Matrices",
                "description": "Vegetation type vs fire severity analysis matrices",
                "keywords": ["vegetation", "fire", "severity", "matrix", "analysis"],
            },
        }

    @classmethod
    def for_testing(cls, base_url: str, storage: StorageInterface) -> "STACCatalogManager":
        """Create a catalog manager instance configured for testing"""
        return cls(base_url=base_url, storage=storage)

    @classmethod
    def for_production(cls, base_url: str, storage: StorageInterface) -> "STACCatalogManager":
        """Create a catalog manager instance configured for production"""
        return cls(base_url=base_url, storage=storage)

    async def initialize_catalog(self) -> pystac.Catalog:
        """
        Initialize the root catalog and collections if they don't exist

        Returns:
            The root catalog
        """
        # Create root catalog
        root_catalog = pystac.Catalog(
            id="fire-recovery-catalog",
            description="Fire Recovery Analysis Results Catalog",
            title="Fire Recovery Analysis Catalog",
        )

        # Add self link to make it a published catalog
        root_catalog.add_link(
            pystac.Link(rel=pystac.RelType.SELF, target=f"{self.base_url}/catalog.json")
        )

        # Create and add collections
        for collection_config in self.collections.values():
            collection = await self._create_collection(collection_config)
            root_catalog.add_child(collection)

        # Save root catalog
        await self._save_catalog_object(root_catalog, "catalog.json")

        return root_catalog

    async def _create_collection(self, config: Dict[str, Any]) -> pystac.Collection:
        """
        Create a STAC collection with proper metadata

        Args:
            config: Collection configuration

        Returns:
            Created collection
        """
        # Create a broad extent that will be updated as items are added
        # This represents the maximum possible extent for fire recovery data
        spatial_extent = pystac.SpatialExtent([[-180.0, -90.0, 180.0, 90.0]])

        # Use a broad temporal extent that covers typical fire seasons
        temporal_extent = pystac.TemporalExtent(
            [
                [datetime(2000, 1, 1), None]  # Open-ended
            ]
        )

        extent = pystac.Extent(spatial_extent, temporal_extent)

        collection = pystac.Collection(
            id=config["id"],
            description=config["description"],
            extent=extent,
            title=config["title"],
            keywords=config["keywords"],
        )

        # Add self link
        collection.add_link(
            pystac.Link(
                rel=pystac.RelType.SELF,
                target=f"{self.base_url}/collections/{config['id']}/collection.json",
            )
        )

        # Add parent link to root
        collection.add_link(
            pystac.Link(
                rel=pystac.RelType.PARENT, target=f"{self.base_url}/catalog.json"
            )
        )

        # Save collection
        collection_path = f"collections/{config['id']}/collection.json"
        await self._save_catalog_object(collection, collection_path)

        return collection

    async def add_fire_severity_item(
        self,
        fire_event_name: str,
        job_id: str,
        cog_urls: Dict[str, str],
        geometry: Polygon | Feature,
        datetime_str: str,
        boundary_type: str = "coarse",
    ) -> Dict[str, Any]:
        """
        Create and add a fire severity item to the catalog

        Args:
            fire_event_name: Name of the fire event
            job_id: Job ID for the processing task
            cog_urls: Dictionary of COG URLs
            geometry: GeoJSON geometry object
            datetime_str: Timestamp for the item
            boundary_type: Type of boundary ('coarse' or 'refined')

        Returns:
            The created STAC item dictionary
        """
        # Create the item using the factory
        item_dict = self.item_factory.create_fire_severity_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            cog_urls=cog_urls,
            geometry=geometry,
            datetime_str=datetime_str,
            boundary_type=boundary_type,
            skip_validation=False,  # We want validation in production
        )

        # Convert to pystac Item for proper link management
        item = pystac.Item.from_dict(item_dict)

        # Add to collection
        await self._add_item_to_collection(item, "fire-severity")

        return item.to_dict(
            transform_hrefs=True
        )  # Transform for proper catalog structure

    async def add_boundary_item(
        self,
        fire_event_name: str,
        job_id: str,
        boundary_geojson_url: str,
        bbox: List[float],
        datetime_str: str,
        boundary_type: str = "coarse",
    ) -> Dict[str, Any]:
        """
        Create and add a boundary item to the catalog

        Args:
            fire_event_name: Name of the fire event
            job_id: Job ID for the processing task
            boundary_geojson_url: URL to the boundary GeoJSON file
            bbox: Bounding box [minx, miny, maxx, maxy]
            datetime_str: Timestamp for the item
            boundary_type: Type of boundary ('coarse' or 'refined')

        Returns:
            The created STAC item dictionary
        """
        item_dict = self.item_factory.create_boundary_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            boundary_geojson_url=boundary_geojson_url,
            bbox=bbox,
            datetime_str=datetime_str,
            boundary_type=boundary_type,
            skip_validation=False,
        )

        item = pystac.Item.from_dict(item_dict)
        await self._add_item_to_collection(item, "fire-boundaries")

        return item.to_dict(transform_hrefs=True)

    async def add_veg_matrix_item(
        self,
        fire_event_name: str,
        job_id: str,
        fire_veg_matrix_csv_url: str,
        fire_veg_matrix_json_url: str,
        geometry: Polygon | Feature,
        bbox: List[float],
        classification_breaks: List[float],
        datetime_str: str,
    ) -> Dict[str, Any]:
        """
        Create and add a vegetation matrix item to the catalog

        Args:
            fire_event_name: Name of the fire event
            job_id: Job ID for the processing task
            fire_veg_matrix_csv_url: URL to the CSV matrix file
            fire_veg_matrix_json_url: URL to the JSON matrix file
            geometry: GeoJSON geometry object
            bbox: Bounding box [minx, miny, maxx, maxy]
            classification_breaks: Classification break values
            datetime_str: Timestamp for the item

        Returns:
            The created STAC item dictionary
        """
        item_dict = self.item_factory.create_veg_matrix_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            fire_veg_matrix_csv_url=fire_veg_matrix_csv_url,
            fire_veg_matrix_json_url=fire_veg_matrix_json_url,
            geometry=geometry,
            bbox=bbox,
            classification_breaks=classification_breaks,
            datetime_str=datetime_str,
            skip_validation=False,
        )

        item = pystac.Item.from_dict(item_dict)
        await self._add_item_to_collection(item, "vegetation-matrices")

        return item.to_dict(transform_hrefs=True)

    async def _add_item_to_collection(
        self, item: pystac.Item, collection_id: str
    ) -> None:
        """
        Add an item to a collection and save both

        Args:
            item: The STAC item to add
            collection_id: ID of the collection to add to
        """
        # Load the existing collection
        collection_path = f"collections/{collection_id}/collection.json"
        try:
            collection_dict = await self.storage.get_json(f"stac/{collection_path}")
            collection = pystac.Collection.from_dict(collection_dict)
        except Exception:
            # Collection doesn't exist, create it
            collection = await self._create_collection(self.collections[collection_id])

        # Set up proper links for the item
        item.add_link(
            pystac.Link(
                rel=pystac.RelType.COLLECTION,
                target=f"{self.base_url}/collections/{collection_id}/collection.json",
            )
        )

        item.add_link(
            pystac.Link(
                rel=pystac.RelType.PARENT,
                target=f"{self.base_url}/collections/{collection_id}/collection.json",
            )
        )

        item.add_link(
            pystac.Link(rel=pystac.RelType.ROOT, target=f"{self.base_url}/catalog.json")
        )

        # Add item to collection (vanilla pystac)
        collection.add_item(item)

        # Save the item
        item_path = f"collections/{collection_id}/items/{item.id}.json"
        await self._save_catalog_object(item, item_path)

        # Update and save the collection with the new item as a link
        await self._save_catalog_object(collection, collection_path)

    async def _save_catalog_object(self, obj: pystac.STACObject, path: str) -> str:
        """
        Save a STAC object to storage

        Args:
            obj: The STAC object to save
            path: Storage path for the object

        Returns:
            Storage URL
        """
        obj_dict = obj.to_dict(transform_hrefs=True)
        # Ensure all STAC catalog files are saved under stac/ prefix
        stac_path = f"stac/{path}"
        return await self.storage.save_json(obj_dict, stac_path)

    async def get_catalog(self) -> Optional[Dict[str, Any]]:
        """
        Get the root catalog

        Returns:
            Root catalog dictionary or None if not found
        """
        try:
            return await self.storage.get_json("stac/catalog.json")
        except Exception:
            return None

    async def get_collection(self, collection_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a collection by ID

        Args:
            collection_id: Collection identifier

        Returns:
            Collection dictionary or None if not found
        """
        try:
            return await self.storage.get_json(
                f"stac/collections/{collection_id}/collection.json"
            )
        except Exception:
            return None

    async def get_collection_items(self, collection_id: str) -> List[Dict[str, Any]]:
        """
        Get all items in a collection

        Args:
            collection_id: Collection identifier

        Returns:
            List of item dictionaries
        """
        items = []
        try:
            item_files = await self.storage.list_files(
                f"stac/collections/{collection_id}/items/"
            )
            for file_path in item_files:
                if file_path.endswith(".json"):
                    item_data = await self.storage.get_json(file_path)
                    items.append(item_data)
        except Exception:
            pass

        return items
