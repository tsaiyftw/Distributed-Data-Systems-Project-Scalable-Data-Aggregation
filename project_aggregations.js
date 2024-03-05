// aggregation 1
db.sfpd_incidents.aggregate([
  {
    $match: {
      category: {
        $nin: ["NON-CRIMINAL", "SECONDARY CODES", "WARRANTS"]
      },
      timestamp: {
        $gte: ISODate("2016-01-01"),
        $lt: ISODate("2017-01-01")
      }
    }
  },
  {
    $group: {
      _id: null,
      total: { $sum: 1 },
      stolenCount: {
        $sum: {
          $cond: {
            if: { $regexMatch: { input: "$descript", regex: /STOLEN AUTOMOBILE/i } },
            then: 1,
            else: 0
          }
        }
      },
      theftCount: {
        $sum: {
          $cond: {
            if: { $regexMatch: { input: "$descript", regex: /THEFT FROM LOCKED AUTO/i } },
            then: 1,
            else: 0
          }
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      stolen_pct: { $round: [{ $multiply: [{ $divide: ["$stolenCount", "$total"] }, 100] }] },
      theft_pct: { $round: [{ $multiply: [{ $divide: ["$theftCount", "$total"] }, 100] }] }
    }
  }
])

// aggregation 2
db.sfpd_incidents.aggregate([
  {
    $match: {
      category: "LARCENY/THEFT",
      descript: /THEFT FROM LOCKED AUTO/i,
      timestamp: {
        $gte: ISODate("2016-01-01"),
        $lt: ISODate("2017-01-01")
      }
    }
  },
  {
    $group: {
      _id: {
        dayOfWeek: { $dayOfWeek: "$timestamp" },
        street: "$address"
      },
      count: { $sum: 1 }
    }
  },
  {
    $sort: { count: -1 }
  },
  {
    $group: {
      _id: "$_id.dayOfWeek",
      topStreets: { $push: { street: "$_id.street", count: "$count" } },
      totalThefts: { $sum: "$count" }
    }
  },
  {
    $project: {
      _id: 0,
      dayOfWeek: "$_id",
      topStreets: { $slice: ["$topStreets", 5] },
      totalThefts: 1
    }
  },
  {
    $sort: { dayOfWeek: 1 }
  }
])

// aggregation 3
db.sfpd_incidents.aggregate([
  {
    $match: {
      category: "LARCENY/THEFT",
      descript: /THEFT FROM LOCKED AUTO/i,
      timestamp: {
        $gte: ISODate("2016-01-01"),
        $lt: ISODate("2017-01-01")
      }
    }
  },
  {
    $group: {
      _id: {
        hour: { $hour: "$timestamp" }
      },
      count: { $sum: 1 }
    }
  },
  {
    $sort: { "_id.hour": 1 }
  }
])
