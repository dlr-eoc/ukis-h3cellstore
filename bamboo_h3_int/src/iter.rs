use std::iter::Zip;

/// an iterator which repeats items according to the given `repetitions` slice
pub struct ItemRepeatingIterator<'a, I, T>
where
    I: Iterator<Item = T>,
    T: Clone,
{
    total_num: Option<usize>,
    zipped_iterator: Zip<I, std::slice::Iter<'a, usize>>,
    repetitions: &'a [usize],

    current_element: Option<I::Item>,
    repetitions_left: usize,
}

impl<'a, I, T> ItemRepeatingIterator<'a, I, T>
where
    I: Iterator<Item = T>,
    T: Clone,
{
    ///
    /// `total_num` is the total number of items this iterator is expected to yield
    pub fn new(iter: I, repetitions: &'a [usize], total_num: Option<usize>) -> Self {
        Self {
            total_num,
            zipped_iterator: iter.zip(repetitions.iter()),
            repetitions,
            current_element: None,
            repetitions_left: 0,
        }
    }
}

impl<'a, I, T> Iterator for ItemRepeatingIterator<'a, I, T>
where
    I: Iterator<Item = T>,
    T: Clone,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.repetitions_left > 0 {
            self.repetitions_left -= 1;
            self.current_element.clone()
        } else {
            loop {
                if let Some((current_element, repetitions_left)) = self.zipped_iterator.next() {
                    if repetitions_left == &0 {
                        continue;
                    }
                    self.current_element = Some(current_element);
                    self.repetitions_left = repetitions_left - 1;
                    return self.current_element.clone();
                } else {
                    return None;
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // this method take care of an efficient memory allocation when `collect`ing
        // the output of this iterator.
        (
            self.total_num
                .unwrap_or_else(|| self.repetitions.iter().sum::<usize>()),
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::iter::ItemRepeatingIterator;

    #[test]
    fn item_repeating_iterator() {
        let some_data = vec![1, 2, 3, 4];
        let repetitions = vec![2, 1, 3, 0];
        let mut iter = ItemRepeatingIterator::new(some_data.iter(), &repetitions, Some(6));
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn item_repeating_iterator_no_num() {
        let some_data = vec![1, 2, 3, 4];
        let repetitions = vec![2, 1, 3, 0];
        let mut iter = ItemRepeatingIterator::new(some_data.iter(), &repetitions, None);
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn item_repeating_iterator_different_lengths() {
        let some_data = vec![1, 2, 3, 4];
        let repetitions = vec![2, 1];
        let mut iter = ItemRepeatingIterator::new(some_data.iter(), &repetitions, None);
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), None);
    }
}
