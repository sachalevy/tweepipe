from tweepipe.utils import locations
from tweepipe.utils.migration.versions import ApiVersion


class Query:
    def __init__(
        self,
        stream: bool = False,
        search: bool = True,
        api_version: str = ApiVersion.STANDARD_V1a1,
    ):
        """
        _summary_

        Args:
            stream (bool, optional): Whether want to construct a query for stream. Defaults to False.
            search (bool, optional): Whether want to construct a query for search. Defaults to True.
            api_version (str, optional): API version informs how to structure queries. Defaults to ApiVersion.STANDARD_V1a1.
        """
        self.query_args = []
        self.api_version = api_version
        self.stream = stream
        self.search = search

    def add_keywords(self, keywords: list):
        self.query_args.append(" OR ".join(keywords))

    def add_user(self, username: str):
        self.query_args.append(f"from:{username}")

    ######## TWEETS GEOTAGGED QUERY OPERATORS ########

    def add_place(
        self,
    ):
        if (
            self.api_version != ApiVersion.ACADEMIC_V2
            and self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2
        ):
            raise ValueError("Cannot use place filter operator.")
        if not self.search:
            raise ValueError("Can only use place filter when searching.")
        return

    def add_point_radius(self, coordinates: tuple, radius: float):
        if (
            self.api_version != ApiVersion.ACADEMIC_V2
            and self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2
        ):
            raise ValueError("Cannot use point radius filter operator.")
        if not self.search:
            raise ValueError("Can only use point radius filter when searching.")

        point_radius_str = f"{coordinates[0]},{coordinates[1]},{radius}km"
        self.query_args.append(point_radius_str)

    def add_bounding_box(self, bounding_box: list):
        if (
            self.api_version != ApiVersion.ACADEMIC_V2
            and self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2
        ):
            raise ValueError("Cannot use geo filter operator.")
        if not self.search:
            raise ValueError("Can only use geo filter when searching.")
        if len(bounding_box) != 4:
            raise ValueError("Bounding box size must be 4.")

        # filter using bounding boxes
        bbox_str = list(map(str, bounding_box)).join(" ")

        self.query_args.append(f"bounding_box:{bbox_str}")

    def add_tweet_geo_filter(self):
        if (
            self.api_version != ApiVersion.ACADEMIC_V2
            and self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2
        ):
            raise ValueError("Cannot use geo filter operator.")
        if not self.search:
            raise ValueError("Can only use geo filter when searching.")

        self.query_args.append("has:geo")

    def add_tweet_geocode(self, coordinates: tuple, radius: float):
        """Add a coordinate and radius to filter geotagged tweets."""

        if self.api_version != ApiVersion.STANDARD_V1a1:
            raise ValueError("Cannot use geocode operator without Standard v1.1 API.")
        if not self.search:
            raise ValueError("Can only use geocode operator when searching tweets.")

        self.query_args.append(f"{coordinates[0]},{coordinates[1]},{radius}km")

    def add_tweet_locations(self, bounding_boxes: list):
        """Add a list of bounding boxes to the chain of query operators."""

        if self.api_version != ApiVersion.STANDARD_V1a1:
            raise ValueError("Cannot use locations operator without Standard v1.1 API.")
        if not self.stream:
            raise ValueError("Need to be streaming data to use this operator.")

        tmp_bbox = ""
        for bbox in bounding_boxes:
            if len(bbox) != 4:
                raise ValueError("Wrong number of coordinates.")
            tmp_bbox_str = list(map(str, bbox)).join(",") + ","
            tmp_bbox += tmp_bbox_str

        self.query_args.append(tmp_bbox)

    def add_place_country(self, country: str):
        if (
            self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2
            and self.api_version != ApiVersion.ACADEMIC_V2
        ):
            raise ValueError(
                "Cannot use profile country without Enterprise or Academic API."
            )
        if country not in locations.country_isos:
            raise ValueError("Country ISO code is not recognized.")
        if not self.search:
            raise ValueError("Can only use place country when searching tweets.")

        self.query_args.append(f"place_country:{country}")

    ######## PROFILE GEOLOCATION QUERY OPERATORS ########
    def add_bio_location_keyword(self, keyword: str):
        if self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2:
            raise ValueError("Cannot use profile country without Enterprise API.")
        self.query_args.append(f"bio_location:{keyword}")

    def add_bio_keyword(self, keyword: str):
        if self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2:
            raise ValueError("Cannot use profile country without Enterprise API.")
        self.query_args.append(f"bio:{keyword}")

    def add_profile_region(self, region: str):
        if self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2:
            raise ValueError("Cannot use profile region without Enterprise API.")

        self.query_args.append(f"profile_region:{region}")

    def add_profile_locality(self, locality: str):
        if self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2:
            raise ValueError("Cannot use profile country without Enterprise API.")
        pass

    def add_profile_sub_region(self, sub_region: str):
        if self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2:
            raise ValueError("Cannot use profile country without Enterprise API.")
        pass

    def add_profile_country(self, country: str):
        if self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2:
            raise ValueError("Cannot use profile country without Enterprise API.")
        if country not in locations.country_isos:
            raise ValueError("Country ISO code is not recognized.")

        self.query_args.append(f"profile_country:{country}")

    def add_profile_geo_filter(
        self,
    ):
        if self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2:
            raise ValueError("Cannot use profile country without Enterprise API.")

        self.query_args.append(f"has:profile_geo")

    def add_profile_bounding_box(self, bbox: list):
        if self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2:
            raise ValueError("Cannot use profile country without Enterprise API.")

        bbox_str = list(map(str, bbox))
        # TODO: check if should join them with spaces or commas
        self.query_args.append(bbox_str.join(","))

    def add_profile_point_radius(self, coordinates: tuple, radius: float):
        """Specify the search coordinates (latitude, longitude) and radius. Note that the radius should
        be specified in kilometers."""
        if self.api_version != ApiVersion.ENTERPRISE_POWERTRACK_V2:
            raise ValueError("Cannot use profile country without Enterprise API.")

        # TODO: check format for the point radius format
        point_radius_str = f"{coordinates[0]},{coordinates[1]},{radius}km"
        self.query_args.append(point_radius_str)

    def get_query_str(self):
        return self.query_args.join(" ")
