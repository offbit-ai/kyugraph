/// Calendar interval: months + days + microseconds.
/// 16 bytes total, matching the C++ `interval_t` layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Interval {
    pub const ZERO: Self = Self {
        months: 0,
        days: 0,
        micros: 0,
    };

    pub const fn new(months: i32, days: i32, micros: i64) -> Self {
        Self {
            months,
            days,
            micros,
        }
    }

    pub const fn from_months(months: i32) -> Self {
        Self::new(months, 0, 0)
    }

    pub const fn from_days(days: i32) -> Self {
        Self::new(0, days, 0)
    }

    pub const fn from_micros(micros: i64) -> Self {
        Self::new(0, 0, micros)
    }
}

impl std::fmt::Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if self.months != 0 {
            let years = self.months / 12;
            let months = self.months % 12;
            if years != 0 {
                parts.push(format!(
                    "{years} year{}",
                    if years.abs() != 1 { "s" } else { "" }
                ));
            }
            if months != 0 {
                parts.push(format!(
                    "{months} month{}",
                    if months.abs() != 1 { "s" } else { "" }
                ));
            }
        }
        if self.days != 0 {
            parts.push(format!(
                "{} day{}",
                self.days,
                if self.days.abs() != 1 { "s" } else { "" }
            ));
        }
        if self.micros != 0 || parts.is_empty() {
            let total_secs = self.micros / 1_000_000;
            let remaining_micros = self.micros % 1_000_000;
            let hours = total_secs / 3600;
            let mins = (total_secs % 3600) / 60;
            let secs = total_secs % 60;
            if remaining_micros != 0 {
                parts.push(format!(
                    "{hours:02}:{mins:02}:{secs:02}.{remaining_micros:06}"
                ));
            } else {
                parts.push(format!("{hours:02}:{mins:02}:{secs:02}"));
            }
        }
        write!(f, "{}", parts.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_16_bytes() {
        assert_eq!(std::mem::size_of::<Interval>(), 16);
    }

    #[test]
    fn zero() {
        let z = Interval::ZERO;
        assert_eq!(z.months, 0);
        assert_eq!(z.days, 0);
        assert_eq!(z.micros, 0);
    }

    #[test]
    fn constructors() {
        let m = Interval::from_months(14);
        assert_eq!(m.months, 14);
        assert_eq!(m.days, 0);

        let d = Interval::from_days(30);
        assert_eq!(d.days, 30);

        let u = Interval::from_micros(1_000_000);
        assert_eq!(u.micros, 1_000_000);
    }

    #[test]
    fn display_zero() {
        assert_eq!(Interval::ZERO.to_string(), "00:00:00");
    }

    #[test]
    fn display_complex() {
        let iv = Interval::new(14, 5, 3_723_000_000);
        let s = iv.to_string();
        assert!(s.contains("1 year"));
        assert!(s.contains("2 months"));
        assert!(s.contains("5 days"));
        assert!(s.contains("01:02:03"));
    }

    #[test]
    fn equality_and_hash() {
        let a = Interval::new(1, 2, 3);
        let b = Interval::new(1, 2, 3);
        let c = Interval::new(1, 2, 4);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
